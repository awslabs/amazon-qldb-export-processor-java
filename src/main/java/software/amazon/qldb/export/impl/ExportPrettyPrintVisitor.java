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

import com.amazon.ion.IonStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.export.BlockVisitor;
import software.amazon.qldb.export.RevisionVisitor;


/**
 * Pretty-prints every block and revision encountered in a journal export.  Output is sent to
 * LOGGER.info().
 */
public class ExportPrettyPrintVisitor implements BlockVisitor, RevisionVisitor {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExportPrettyPrintVisitor.class);
    
    @Override
    public void setup() {
    }

    @Override
    public void teardown() {
    }

    @Override
    public void visit(IonStruct block) {
        LOGGER.info("BLOCK");
        LOGGER.info(block.toPrettyString());
    }

    @Override
    public void visit(IonStruct revision, String tableName) {
        LOGGER.info("REVISION in table " + tableName);
        LOGGER.info(revision.toPrettyString());
    }
}
