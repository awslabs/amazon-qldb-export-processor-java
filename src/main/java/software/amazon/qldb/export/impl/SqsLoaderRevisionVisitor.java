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

import com.amazon.ion.IonInt;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.qldb.export.RevisionVisitor;
import software.amazon.qldb.load.LoadEvent;


/**
 * Writes a revision into an SQS queue in the format required by the qldb-ledger-load project.
 */
public class SqsLoaderRevisionVisitor implements RevisionVisitor {

    private final static IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();

    private final String queueUrl;
    private final SqsClient sqsClient;
    private final boolean useFifo;


    private SqsLoaderRevisionVisitor(SqsClient sqsClient, String queueUrl) {
        if (sqsClient == null)
            throw new IllegalArgumentException("SQS client cannot be null");

        if (queueUrl == null)
            throw new IllegalArgumentException("SQS Queue URL cannot be null");

        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        useFifo = queueUrl.endsWith(".fifo");
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public SqsClient getSqsClient() {
        return sqsClient;
    }

    @Override
    public void setup() {
    }

    @Override
    public void visit(IonStruct revision, String tableName) {
        LoadEvent event = LoadEvent.fromCommittedRevision(revision, tableName);

        SendMessageRequest.Builder builder = SendMessageRequest.builder();
        builder.queueUrl(queueUrl);
        builder.messageBody(event.toPrettyString());

        if (useFifo) {
            builder.messageGroupId(event.getId().toString());
        }

        SendMessageResponse response = sqsClient.sendMessage(builder.build());
    }

    @Override
    public void teardown() {
    }

    public static SqsLoaderRevisionVisitorBuilder builder() {
        return SqsLoaderRevisionVisitorBuilder.builder();
    }


    public static class SqsLoaderRevisionVisitorBuilder {
        private String queueUrl;
        private SqsClient sqsClient;

        public static SqsLoaderRevisionVisitorBuilder builder() {
            return new SqsLoaderRevisionVisitorBuilder();
        }

        public SqsLoaderRevisionVisitorBuilder queueUrl(String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        public SqsLoaderRevisionVisitorBuilder sqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public SqsLoaderRevisionVisitor build() {
            return new SqsLoaderRevisionVisitor(sqsClient, queueUrl);
        }
    }
}
