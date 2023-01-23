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
package software.amazon.qldb.export.app;

import org.apache.commons.cli.*;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.awssdk.services.qldbsession.QldbSessionClientBuilder;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.export.ExportProcessor;
import software.amazon.qldb.export.impl.SequentialLedgerLoadBlockVisitor;
import software.amazon.qldb.load.writer.BaseRevisionWriter;

import java.io.PrintWriter;
import java.util.Arrays;


/**
 * Loads data from a QLDB export into a ledger sequentially. Revisions are written into the ledger using one database
 * transaction per block to mimic the transaction history from the export.
 *
 * Note that a single ledger writer process can only write 10-20 transactions per second, making a sequential approach
 * usable only for very small ledgers.  Consider using an asynchronous approach for medium to large ledgers.  See the
 * qldb-ledger-load-java project that supports several data delivery channels.
 */
public class SequentialExportLoader {
    public static void main(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder("t")
                .required()
                .desc("Name of target ledger")
                .longOpt("target-ledger")
                .hasArg()
                .build());

        //
        // There are two ways to find the export(s) we need to process.
        //
        // First method:
        //
        // Find the location of the export manifest file by querying the QLDB service
        // with a source ledger name and the ID of the export to process.  The source
        // ledger must exist for this option.
        //
        options.addOption(Option.builder("s")
                .desc("Name of source ledger")
                .longOpt("source-ledger")
                .hasArg()
                .build());

        options.addOption(Option.builder("x")
                .desc("Export ID")
                .longOpt("export-id")
                .hasArg()
                .build());

        //
        // Second method:  Accept a bucket name and path(s) to the manifest file(s).
        //
        options.addOption(Option.builder("b")
                .desc("Name of S3 bucket containing exports")
                .longOpt("bucket")
                .hasArg()
                .build());

        options.addOption(Option.builder("mp")
                .desc("S3 path (key) to a completed export manifest file. Specify this argument multiple times to process multiple exports that cover a contiguous, non-overlapping set of blocks from the same ledger.")
                .longOpt("manifest")
                .hasArg()
                .build());

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine line = parser.parse(options, args);

            QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
            QldbDriver driver = QldbDriver.builder()
                    .ledger(line.getOptionValue("t"))
                    .sessionClientBuilder(sessionClientBuilder)
                    .build();

            BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(driver).build();
            SequentialLedgerLoadBlockVisitor visitor = SequentialLedgerLoadBlockVisitor.builder().writer(writer).build();
            ExportProcessor processor = ExportProcessor.builder()
                    .blockVisitor(visitor)
                    .build();

            if (line.hasOption("s") && line.hasOption("x")) {
                processor.process(line.getOptionValue("s"), line.getOptionValue("x"));
            } else if (line.hasOption("b") && line.hasOption("mp")) {
                String[] values = line.getOptionValues("mp");
                if (values.length == 1)
                    processor.processExport(line.getOptionValue("b"), values[0]);
                else {
                    processor.processExports(line.getOptionValue("b"), Arrays.asList(values));
                }
            } else {
                printUsage("Missing one or more required options", options);
                System.exit(-1);
            }
        } catch (ParseException pe) {
            printUsage(pe.getMessage(), options);
        }
    }


    private static void printUsage(String message, Options options) {
        try (PrintWriter pw = new PrintWriter(System.err)) {
            pw.println(message);

            HelpFormatter formatter = new HelpFormatter();

            pw.println("\nRequired:");
            Options ops = new Options();
            ops.addOption(options.getOption("t"));
            formatter.printOptions(pw, formatter.getWidth(), ops, formatter.getLeftPadding(), formatter.getLeftPadding());

            pw.println("\nSpecify both of these:");
            ops = new Options();
            ops.addOption(options.getOption("s"));
            ops.addOption(options.getOption("x"));
            formatter.printOptions(pw, formatter.getWidth(), ops, formatter.getLeftPadding(), formatter.getLeftPadding());

            pw.println("\nOr both of these:");
            ops = new Options();
            ops.addOption(options.getOption("b"));
            ops.addOption(options.getOption("mp"));
            formatter.printOptions(pw, formatter.getWidth(), ops, formatter.getLeftPadding(), formatter.getLeftPadding());
        }
    }
}
