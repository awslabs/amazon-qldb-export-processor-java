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

import com.amazon.ion.IonStruct;


/**
 * Processes a single journal revision.  Invoked from the export processor for each revision encountered in a QLDB ledger
 * export.
 */
public interface RevisionVisitor {
    /**
     * Performs any setup or initialization work of the visitor prior to processing a ledger export.  Invoked once
     * per export.
     */
    public void setup();


    /**
     * Invoked by the export processor once for each revision encountered in the ledger export.
     *
     * @param revision The revision to process
     * @param tableId The ID of the ledger table the revision belongs to.  This can be used to distinguish
     *                between dropped tables that have the same name, or a dropped table with the same name
     *                as an active table.
     * @param tableName The name of the ledger table the revision belongs to
     */
    public void visit(IonStruct revision, String tableId, String tableName);


    /**
     * Performs any tear down work of the visitor after the ledger export has been processed.  Invoked once per export.
     */
    public void teardown();
}
