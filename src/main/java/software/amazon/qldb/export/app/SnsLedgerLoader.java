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

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.qldb.export.ExportProcessor;
import software.amazon.qldb.export.impl.SnsLoaderRevisionVisitor;

import java.io.IOException;


/**
 * Sends revisions from a QLDB export into an SNS topic for loading asynchronously into a QLDB ledger.
 *
 * The qldb-ledger-load project receives the events and writes them to the ledger.
 */
public class SnsLedgerLoader {
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage:  SnsLedgerLoader sourceLedgerName exportId topicArn");
            System.exit(-1);
        }

        SnsClient sns = SnsClient.builder().build();
        SnsLoaderRevisionVisitor visitor = SnsLoaderRevisionVisitor.builder()
                .topicArn(args[2])
                .snsClient(sns)
                .build();

        ExportProcessor processor = ExportProcessor.builder()
                .revisionVisitor(visitor)
                .build();

        processor.process(args[0], args[1]);
    }
}
