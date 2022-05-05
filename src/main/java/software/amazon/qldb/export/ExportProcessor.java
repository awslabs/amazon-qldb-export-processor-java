/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package software.amazon.qldb.export;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.util.*;


/**
 * Iterates over the journal blocks in a QLDB export, calling plug-ins to process the blocks and revisions in
 * the export.  Instances of {@link software.amazon.qldb.export.BlockVisitor} and
 * {@link software.amazon.qldb.export.RevisionVisitor} can be plugged-in to this processor and perform the
 * actual work required for the export.
 */
public class ExportProcessor {
    private final IonSystem ionSystem = IonSystemBuilder.standard().build();
    private final S3Client s3Client;
    private final QldbClient qldbClient;

    private final BlockVisitor blockVisitor;
    private final RevisionVisitor revisionVisitor;

    private final int startBlock;
    private final int endBlock;

    // Runtime counter
    private int currentBlockNum = -1;


    private ExportProcessor(S3Client s3Client, QldbClient qldbClient, BlockVisitor blockVisitor,
                            RevisionVisitor revisionVisitor, int startBlock, int endBlock) {
        this.s3Client = s3Client == null ? S3Client.builder().build() : s3Client;
        this.qldbClient = qldbClient == null ? QldbClient.builder().build() : qldbClient;
        this.blockVisitor = blockVisitor;
        this.revisionVisitor = revisionVisitor;
        this.startBlock = startBlock;
        this.endBlock = endBlock;
    }


    /**
     * Processes an export given a ledger name and export ID.  The export must be accessible through the
     * AWS QLDB SDK.  Its S3 storage location will be retrieved via the SDK.
     *
     * @param ledgerName The name of the ledger
     * @param exportId The ID of the export to process.
     */
    public void process(String ledgerName, String exportId) {
        if (exportId == null)
            throw new IllegalArgumentException("Export ID required");

        if (ledgerName == null)
            throw new IllegalArgumentException("Ledger name is required");

        DescribeJournalS3ExportRequest req = DescribeJournalS3ExportRequest.builder().exportId(exportId).name(ledgerName).build();
        DescribeJournalS3ExportResponse resp = qldbClient.describeJournalS3Export(req);
        JournalS3ExportDescription description = resp.exportDescription();

        if (description.status() != ExportStatus.COMPLETED)
            throw new RuntimeException("Export has not completed");

        GetDigestRequest digestRequest = GetDigestRequest.builder().name(ledgerName).build();
        GetDigestResponse digestResponse = qldbClient.getDigest(digestRequest);
        IonStruct tip = (IonStruct) ionSystem.singleValue(digestResponse.digestTipAddress().ionText());
        String strandId = ((IonString) tip.get("strandId")).stringValue();

        S3ExportConfiguration s3Info = description.s3ExportConfiguration();

        String manifestName = s3Info.prefix() + exportId + "." + strandId + ".completed.manifest";
        ListObjectsV2Request s3Req = ListObjectsV2Request.builder().bucket(s3Info.bucket()).prefix(manifestName).build();
        ListObjectsV2Response s3Resp = s3Client.listObjectsV2(s3Req);

        if (s3Resp.hasContents()) {
            S3Object obj = s3Resp.contents().get(0);
            processExport(s3Req.bucket(), obj.key());
        }
    }


    /**
     * Processes an export given the location of the completed manifest file in S3.
     *
     * @param bucket The bucket where the export files are located
     * @param manifestPath The S3 key (path) of the completed manifest file.
     */
    public void processExport(String bucket, String manifestPath) {

        if (bucket == null)
            throw new IllegalArgumentException("S3 bucket name required");

        if (manifestPath == null)
            throw new IllegalArgumentException("S3 path for manifest file is required");

        if (!manifestPath.endsWith(".completed.manifest"))
            throw new IllegalArgumentException("Completed manifest path should end in .completed.manifest");

        try {
            setup();

            boolean endBlockReached = false;

            for (String key : getExportFileKeys(bucket, manifestPath)) {

                // Don't bother fetching this file from S3 if its blocks are out-of-range
                if ((this.startBlock > -1 || this.endBlock > -1) && !blocksInRange(key)) {
                    continue;
                }

                List<IonStruct> blocks = getJournalBlocks(bucket, key);
                for (IonStruct block : blocks) {

                    IonStruct blockAddress = (IonStruct) block.get("blockAddress");
                    currentBlockNum = ((IonInt) blockAddress.get("sequenceNo")).intValue();

                    if (this.startBlock > -1 && currentBlockNum < startBlock)
                        continue;

                    if (this.endBlock > -1 && currentBlockNum > endBlock) {
                        endBlockReached = true;
                        break;
                    }

                    if (blockVisitor != null) {
                        blockVisitor.visit(block);
                    }

                    if (revisionVisitor == null)
                        continue;

                    if (!block.containsKey("revisions"))
                        continue;

                    if (!block.containsKey("transactionInfo"))
                        continue;

                    IonStruct txInfo = (IonStruct) block.get("transactionInfo");
                    if (!txInfo.containsKey("documents"))
                        continue;

                    Map<String, String> tableMapping = new HashMap<>();

                    //
                    // The revisions in the block don't contain information about the table they are in.  Instead, the
                    // 'transactionInfo' section of the block has the mapping of the table to the document ID.  Get that
                    // mapping for use below.
                    //
                    IonStruct txDocInfo = (IonStruct) txInfo.get("documents");
                    for (IonValue value : txDocInfo) {
                        IonStruct val = (IonStruct) value;
                        tableMapping.put(val.getFieldName(), ((IonString) val.get("tableName")).stringValue());
                    }

                    //
                    // Now iterate over the revisions in the block.
                    //
                    for (IonValue ionValue : (IonList) block.get("revisions")) {
                        IonStruct revision = (IonStruct) ionValue;

                        if (!revision.containsKey("metadata"))
                            continue;

                        IonStruct metadata = (IonStruct) revision.get("metadata");
                        String documentId = ((IonString) metadata.get("id")).stringValue();

                        String table = tableMapping.get(documentId);
                        if (table == null)
                            table = "***UNKNOWN***";

                        revisionVisitor.visit(revision, table);
                    }
                }

                if (endBlockReached)
                    break;
            }
        } catch (Exception e) {
            if (currentBlockNum < 0) {
                throw new RuntimeException("Processing failed prior to first block", e);
            } else {
                throw new RuntimeException("Processing failed at block " + currentBlockNum, e);
            }
        } finally {
            teardown();
        }
    }


    /**
     * Fetches the S3 keys for all of the block files in the export from the completed manifest
     * file.
     *
     * @param bucket  The name of the S3 bucket where the export files live.
     * @param path  The S3 key (path) of the completed manifest file
     * @return A list of the S3 keys of each of the export data files
     * @throws IOException For problems reading objects from S3.
     */
    private List<String> getExportFileKeys(String bucket, String path) throws IOException {
        ArrayList<String> keys = new ArrayList<>();

        if (path.startsWith("/"))
            path = path.substring(1);

        GetObjectRequest gor = GetObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .build();

        try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(gor);
             IonReader ionReader = ionSystem.newReader(stream)) {

            ionReader.next();
            IonStruct ionStruct = (IonStruct) ionSystem.newValue(ionReader);
            IonList ionKeysList = (IonList) ionStruct.get("keys");
            ionKeysList.forEach(key -> keys.add(((IonString) key).stringValue()));

            return keys;
        }
    }


    /**
     * A single export file may contain multiple journal blocks.  Reads all of the blocks from a single
     * export file from S3.
     *
     * @param bucketName  The name of the bucket where the export data file lives
     * @param key   The S3 key (path) of the export data file to read
     * @return A list of all of the journal blocks in the export data file.
     * @throws IOException For problems reading the file from S3.
     */
    private List<IonStruct> getJournalBlocks(String bucketName, String key) throws IOException {
        List<IonStruct> blocks = new ArrayList<>();

        GetObjectRequest gor = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(gor);
             IonReader ionReader = ionSystem.newReader(stream)) {

            IonLoader loader = ionSystem.getLoader();
            IonDatagram gram = loader.load(ionReader);

            for (IonValue ionValue : gram) {
                blocks.add((IonStruct) ionValue);
            }
        }

        return blocks;
    }


    /**
     * Identifies whether or not an export data file should be fetched from S3, given the blocks
     * it contains.  If the start and/or end blocks are configured for this export processor, data
     * files will be skipped if their blocks are not in the desired range of blocks to process.
     */
    private boolean blocksInRange(String s3Key) {
        if (!s3Key.matches("^.*\\.[0-9]*-[0-9]*\\.[a-zA-Z0-9]*$"))
            return false;

        String[] parts = s3Key.split("\\.");
        String[] parts2 = parts[parts.length - 2].split("-");

        if (this.endBlock > -1 && Integer.parseInt(parts2[0]) > this.endBlock)
            return false;

        if (this.startBlock > -1 && Integer.parseInt(parts2[1]) < this.startBlock)
            return false;

        return true;
    }


    private void setup() {
        if (revisionVisitor != null)
            revisionVisitor.setup();

        if (blockVisitor != null)
            blockVisitor.setup();
    }


    private void teardown() {
        if (revisionVisitor != null)
            revisionVisitor.teardown();

        if (blockVisitor != null)
            blockVisitor.teardown();
    }


    public static ExportProcesserBuilder builder() {
        return ExportProcesserBuilder.builder();
    }


    public static class ExportProcesserBuilder {
        private S3Client s3Client = S3Client.builder().build();
        private QldbClient qldbClient = QldbClient.builder().build();

        private BlockVisitor blockVisitor;
        private RevisionVisitor revisionVisitor;

        private int startBlock = -1;
        private int endBlock = -1;

        private ExportProcesserBuilder() {}

        public static ExportProcesserBuilder builder() {
            return new ExportProcesserBuilder();
        }


        public ExportProcesserBuilder blockVisitor(BlockVisitor blockVisitor) {
            this.blockVisitor = blockVisitor;
            return this;
        }


        public ExportProcesserBuilder revisionVisitor(RevisionVisitor revisionVisitor) {
            this.revisionVisitor = revisionVisitor;
            return this;
        }

        public ExportProcesserBuilder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public ExportProcesserBuilder qldbClient(QldbClient qldbClient) {
            this.qldbClient = qldbClient;
            return this;
        }

        /**
         * Provides the ability to process a subset of the blocks in the export.  Sets the minimum block
         * sequence number that will be processed.  Set this to a negative number to disable range checking.
         *
         * @param startBlock The sequence number of the first block in the export to process
         * @return An instance of this builder for method chaining.
         */
        public ExportProcesserBuilder startBlock(int startBlock) {
            this.startBlock = startBlock;
            return this;
        }

        /**
         * Provides the ability to process a subset of the blocks in the export.  Sets the maximum block
         * sequence number that will be processed.  Set this to a negative number to disable range checking.
         *
         * @param endBlock The sequence number of the last block in the export to process
         * @return An instance of this builder for method chaining.
         */
        public ExportProcesserBuilder endBlock(int endBlock) {
            this.endBlock = endBlock;
            return this;
        }

        public ExportProcessor build() {
            return new ExportProcessor(s3Client, qldbClient, blockVisitor, revisionVisitor, startBlock, endBlock);
        }
    }
}
