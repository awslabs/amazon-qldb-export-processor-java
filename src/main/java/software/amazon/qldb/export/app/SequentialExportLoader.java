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

import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.awssdk.services.qldbsession.QldbSessionClientBuilder;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.export.ExportProcessor;
import software.amazon.qldb.export.impl.SequentialLedgerLoadBlockVisitor;
import software.amazon.qldb.load.writer.BaseRevisionWriter;


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
        if (args.length < 3) {
            System.err.println("Usage:  SequentialExportLoader sourceLedgerName exportId targetLedgerName");
            System.exit(-1);
        }

        QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
        QldbDriver driver = QldbDriver.builder()
                .ledger(args[2])
                .sessionClientBuilder(sessionClientBuilder)
                .build();

        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(driver).build();
        SequentialLedgerLoadBlockVisitor visitor = SequentialLedgerLoadBlockVisitor.builder().writer(writer).build();

        ExportProcessor processor = ExportProcessor.builder().blockVisitor(visitor).build();
        processor.process(args[0], args[1]);
    }
}
