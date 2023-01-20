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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.qldb.QldbClient;
import software.amazon.awssdk.services.qldb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * Iterates over the journal blocks in a QLDB export, calling plug-ins to process the blocks and revisions in
 * the export.  Instances of {@link software.amazon.qldb.export.BlockVisitor} and
 * {@link software.amazon.qldb.export.RevisionVisitor} can be plugged-in to this processor and perform the
 * actual work required for the export.
 */
public class ExportProcessor {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExportProcessor.class);

    private final IonSystem ionSystem = IonSystemBuilder.standard().build();
    private final S3Client s3Client;
    private final QldbClient qldbClient;

    private final List<BlockVisitor> blockVisitors = new ArrayList<>();
    private final List<RevisionVisitor> revisionVisitors = new ArrayList<>();

    private final int startBlock;
    private final int endBlock;

    // Runtime counter
    private int currentBlockNum = -1;


    private ExportProcessor(S3Client s3Client, QldbClient qldbClient, List<BlockVisitor> blockVisitors,
                            List<RevisionVisitor> revisionVisitors, int startBlock, int endBlock) {
        this.s3Client = s3Client == null ? S3Client.builder().build() : s3Client;
        this.qldbClient = qldbClient == null ? QldbClient.builder().build() : qldbClient;
        this.blockVisitors.addAll(blockVisitors);
        this.revisionVisitors.addAll(revisionVisitors);
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
            processManifest(bucket, manifestPath);
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
     * Processes multiple exports, given the locations of the completed manifest files in S3.
     *
     * @param bucket The bucket where the export files are located
     * @param manifestPaths List of the S3 keys (paths) of the completed manifest files.
     */
    public void processExports(String bucket, List<String> manifestPaths) {
        if (bucket == null)
            throw new IllegalArgumentException("S3 bucket name required");

        if (manifestPaths == null || manifestPaths.size() == 0)
            throw new IllegalArgumentException("S3 paths for manifest file are required");

        String checkStrand = null;

        List<ManifestStruct> structs = new ArrayList<>();

        // Look over the manifest files for problems before we start a potentially long process.
        try {
            for (String path : manifestPaths) {

                // Make sure we have a path to a completed manifest file
                File file = new File(path);
                String name = file.getName();
                if (!name.matches("\\w+\\.\\w+\\.completed\\.manifest"))
                    throw new IllegalArgumentException("Invalid manifest path \"" + path + "\"");

                // Make sure all the paths relate to the same source ledger strand
                String[] parts = name.split("\\.");
                if (checkStrand == null) {
                    checkStrand = parts[1];
                } else {
                    if (!checkStrand.equals(parts[1]))
                        throw new IllegalArgumentException("Exports must all be from the same strand");

                    checkStrand = parts[1];
                }

                // Fetch the start and end blocks of each manifest.  We'll need this in a minute.
                ManifestStruct struct = getManifestBlockRange(bucket, path);
                if (struct != null)
                    structs.add(struct);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to fetch manifest files from S3", e);
        }

        // Sort the manifests by their starting block number.  Can't trust the caller to give us a sorted list.
        structs.sort(new Comparator<ManifestStruct>() {
            @Override
            public int compare(ManifestStruct o1, ManifestStruct o2) {
                return Integer.compare(o1.firstBlock, o2.firstBlock);
            }
        });

        // Make sure we have a contiguous, non-overlapping set of exports
        for (int i = 1; i < structs.size(); i++) {
            if (structs.get(i).firstBlock != structs.get(i - 1).lastBlock + 1)
                throw new IllegalArgumentException("Manifests overlap or are not contiguous");
        }

        // Now Process the exports in order
        try {
            setup();
            for (ManifestStruct mf : structs) {
                processManifest(bucket, mf.path);
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


    private void processManifest(String bucket, String manifestPath) throws IOException {

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

                if (blockVisitors.size() > 0) {
                    for (BlockVisitor bv : blockVisitors)
                        bv.visit(block);
                }

                if (revisionVisitors.size() == 0)
                    continue;

                if (!block.containsKey("revisions"))
                    continue;

                if (!block.containsKey("transactionInfo"))
                    continue;

                IonStruct txInfo = (IonStruct) block.get("transactionInfo");
                if (!txInfo.containsKey("documents"))
                    continue;

                IonStruct txDocInfo = (IonStruct) txInfo.get("documents");

                //
                // Now iterate over the revisions in the block.
                //
                for (IonValue ionValue : (IonList) block.get("revisions")) {
                    IonStruct revision = (IonStruct) ionValue;

                    if (!revision.containsKey("metadata"))
                        continue;

                    IonStruct metadata = (IonStruct) revision.get("metadata");
                    String documentId = ((IonString) metadata.get("id")).stringValue();

                    String tableName = "***UNKNOWN***";
                    String tableId = "***UNKNOWN***";

                    if (txDocInfo.containsKey(documentId)) {
                        IonStruct tableInfo = (IonStruct) txDocInfo.get(documentId);
                        tableName = ((IonString) tableInfo.get("tableName")).stringValue();
                        tableId = ((IonString) tableInfo.get("tableId")).stringValue();
                    }

                    for (RevisionVisitor rv : revisionVisitors) {
                        rv.visit(revision, tableId, tableName);
                    }
                }
            }

            if (endBlockReached)
                break;
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
     * Identifies whether an export data file should be fetched from S3, given the blocks
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

        return this.startBlock <= -1 || Integer.parseInt(parts2[1]) >= this.startBlock;
    }


    private ManifestStruct getManifestBlockRange(String bucket, String path) throws IOException {
        GetObjectRequest gor = GetObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .build();

        ManifestStruct struct = new ManifestStruct();
        struct.path = path;

        try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(gor);
             IonReader ionReader = ionSystem.newReader(stream)) {

            ionReader.next();
            IonStruct ionStruct = (IonStruct) ionSystem.newValue(ionReader);
            IonList ionKeysList = (IonList) ionStruct.get("keys");
            if (ionKeysList.size() == 0)
                return null;

            IonValue ion = ionKeysList.get(0);
            String key = ((IonString) ion).stringValue();
            String[] parts = key.split("\\.");
            parts = parts[parts.length - 2].split("-");

            struct.firstBlock = Integer.parseInt(parts[0]);

            if (ionKeysList.size() > 1) {
                ion = ionKeysList.get(ionKeysList.size() - 1);
                key = ((IonString) ion).stringValue();
                parts = key.split("\\.");
                parts = parts[parts.length - 2].split("-");
            }

            struct.lastBlock = Integer.parseInt(parts[1]);
        }

        return struct;
    }


    private void setup() {
        for (RevisionVisitor rv : revisionVisitors)
            rv.setup();

        for (BlockVisitor bv : blockVisitors)
            bv.setup();
    }


    private void teardown() {
        for (RevisionVisitor rv : revisionVisitors) {
            try {
                rv.teardown();
            } catch (Exception e) {
                LOGGER.warn("Error tearing down revision visitor " + rv.getClass().getName(), e);
            }
        }

        for (BlockVisitor bv : blockVisitors) {
            try {
                bv.teardown();
            } catch (Exception e) {
                LOGGER.warn("Error tearing down block visitor " + bv.getClass().getName(), e);
            }
        }
    }


    private static class ManifestStruct {
        String path;
        int firstBlock;
        int lastBlock;
    }


    public static ExportProcesserBuilder builder() {
        return ExportProcesserBuilder.builder();
    }


    public static class ExportProcesserBuilder {
        private S3Client s3Client = S3Client.builder().build();
        private QldbClient qldbClient = QldbClient.builder().build();

        private List<BlockVisitor> blockVisitors = new ArrayList<>();
        private List<RevisionVisitor> revisionVisitors = new ArrayList<>();

        private int startBlock = -1;
        private int endBlock = -1;

        private ExportProcesserBuilder() {}

        public static ExportProcesserBuilder builder() {
            return new ExportProcesserBuilder();
        }


        public ExportProcesserBuilder blockVisitor(BlockVisitor blockVisitor) {
            blockVisitors.clear();
            blockVisitors.add(blockVisitor);
            return this;
        }

        public ExportProcesserBuilder blockVisitors(List<BlockVisitor> bvs) {
            blockVisitors.clear();
            blockVisitors.addAll(bvs);
            return this;
        }

        public ExportProcesserBuilder addBlockVisitor(BlockVisitor bv) {
            if (bv != null)
                blockVisitors.add(bv);
            return this;
        }

        public ExportProcesserBuilder revisionVisitor(RevisionVisitor revisionVisitor) {
            revisionVisitors.clear();
            revisionVisitors.add(revisionVisitor);
            return this;
        }

        public ExportProcesserBuilder revisionVisitors(List<RevisionVisitor> rvs) {
            revisionVisitors.clear();
            revisionVisitors.addAll(rvs);
            return this;
        }

        public ExportProcesserBuilder addRevisionVisitor(RevisionVisitor rv) {
            if (rv != null)
                revisionVisitors.add(rv);
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
            return new ExportProcessor(s3Client, qldbClient, blockVisitors, revisionVisitors, startBlock, endBlock);
        }
    }
}
