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
package software.amazon.qldb.export.impl;

import com.amazon.ion.IonList;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import software.amazon.qldb.export.BlockVisitor;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.writer.RevisionWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Loads data from a QLDB export into a ledger sequentially. Revisions are written into the ledger using one database
 * transaction per block to mimic the transaction history from the export.
 *
 * Note that a single ledger writer process can only write 10-20 transactions per second, making a sequential approach
 * usable only for very small ledgers.  Consider using an asynchronous approach for medium to large ledgers.  See the
 * qldb-ledger-load-java project that supports several data delivery channels.
 */
public class SequentialLedgerLoadBlockVisitor implements BlockVisitor {
    private final RevisionWriter writer;


    private SequentialLedgerLoadBlockVisitor(RevisionWriter writer) {
        if (writer == null)
            throw new IllegalArgumentException("Revision writer is required");

        this.writer = writer;
    }


    @Override
    public void setup() {
    }


    @Override
    public void visit(IonStruct block) {
        if (!(block.containsKey("transactionInfo") && block.containsKey("revisions")))
            return;

        IonStruct transactionInfo = (IonStruct) block.get("transactionInfo");

        // This block's transaction did not affect any document revisions, so skip it
        if (!transactionInfo.containsKey("documents"))
            return;

        HashMap<String, String> tableMap = new HashMap<>();
        IonStruct documents = (IonStruct) transactionInfo.get("documents");
        for (IonValue field : documents) {
            String tableName = ((IonString) ((IonStruct) field).get("tableName")).stringValue();
            tableMap.put(field.getFieldName(), tableName);
        }

        // First make sure there's work to do in this block
        List<LoadEvent> events = new ArrayList<>();
        for (IonValue ionValue : (IonList) block.get("revisions")) {
            IonStruct revision = (IonStruct) ionValue;

            if (!revision.containsKey("metadata"))
                continue;

            IonStruct metadata = (IonStruct) revision.get("metadata");
            String docId = ((IonString) metadata.get("id")).stringValue();

            String tableName = tableMap.get(docId);
            LoadEvent event = LoadEvent.fromCommittedRevision(revision, tableName);
            events.add(event);
        }

        writer.writeEvents(events);
    }

    @Override
    public void teardown() {

    }

    public static SequentialLedgerLoadBlockVisitorBuilder builder() {
        return SequentialLedgerLoadBlockVisitorBuilder.builder();
    }


    public static class SequentialLedgerLoadBlockVisitorBuilder {
        private RevisionWriter writer;

        public SequentialLedgerLoadBlockVisitorBuilder writer(RevisionWriter writer) {
            this.writer = writer;
            return this;
        }

        public SequentialLedgerLoadBlockVisitor build() {
            return new SequentialLedgerLoadBlockVisitor(writer);
        }

        public static SequentialLedgerLoadBlockVisitorBuilder builder() {
            return new SequentialLedgerLoadBlockVisitorBuilder();
        }
    }
}
