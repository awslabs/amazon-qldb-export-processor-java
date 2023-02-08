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
import software.amazon.qldb.export.ExportProcessor;
import software.amazon.qldb.export.impl.ExportPrettyPrintVisitor;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;


/**
 * Pretty-prints all of the blocks in a QLDB export.  Output is sent to the logger.
 */
public class ExportBlockPrettyPrinter {

    public static void main(String[] args) throws IOException {
        Options options = new Options();

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

        //
        // Additional options
        //
        options.addOption(Option.builder("start")
                .desc("Number of first block to process")
                .longOpt("startBlock")
                .hasArg()
                .build());

        options.addOption(Option.builder("end")
                .desc("Number of last block to process")
                .longOpt("endBlock")
                .hasArg()
                .build());

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine line = parser.parse(options, args);

            ExportProcessor.ExportProcesserBuilder builder = ExportProcessor.builder()
                    .revisionVisitor(new ExportPrettyPrintVisitor())
                    .blockVisitor(new ExportPrettyPrintVisitor());

            if (line.hasOption("start")) {
                try {
                    builder.startBlock(Integer.parseInt(line.getOptionValue("start")));
                } catch (NumberFormatException nfe) {
                    printUsage("Start block must be numeric", options);
                    System.exit(-1);
                }
            }

            if (line.hasOption("end")) {
                try {
                    builder.endBlock(Integer.parseInt(line.getOptionValue("end")));
                } catch (NumberFormatException nfe) {
                    printUsage("End block must be numeric", options);
                    System.exit(-1);
                }
            }

            ExportProcessor processor = builder.build();

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
            Options ops = new Options();

            pw.println("\nSpecify both of these:");
            ops.addOption(options.getOption("s"));
            ops.addOption(options.getOption("x"));
            formatter.printOptions(pw, formatter.getWidth(), ops, formatter.getLeftPadding(), formatter.getLeftPadding());

            pw.println("\nOr both of these:");
            ops = new Options();
            ops.addOption(options.getOption("b"));
            ops.addOption(options.getOption("mp"));
            formatter.printOptions(pw, formatter.getWidth(), ops, formatter.getLeftPadding(), formatter.getLeftPadding());

            pw.println("\nOptional:");
            ops = new Options();
            ops.addOption(options.getOption("start"));
            ops.addOption(options.getOption("end"));
            formatter.printOptions(pw, formatter.getWidth(), ops, formatter.getLeftPadding(), formatter.getLeftPadding());
        }
    }
}
