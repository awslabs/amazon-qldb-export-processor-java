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

import software.amazon.qldb.export.ExportProcessor;
import software.amazon.qldb.export.impl.ExportPrettyPrintVisitor;

import java.io.IOException;


/**
 * Pretty-prints all of the blocks in a QLDB export.  Output is sent to the logger.
 */
public class ExportBlockPrettyPrinter {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage:  ExportBlockPrettyPrinter ledgerName exportId");
            System.exit(-1);
        }

        ExportProcessor processor = ExportProcessor.builder()
                .blockVisitor(new ExportPrettyPrintVisitor())
                .build();

        processor.process(args[0], args[1]);
    }
}
