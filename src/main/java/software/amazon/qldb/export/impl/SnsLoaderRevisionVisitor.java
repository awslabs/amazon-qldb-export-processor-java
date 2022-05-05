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
import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.qldb.export.RevisionVisitor;
import software.amazon.qldb.load.LoadEvent;


/**
 * Writes a revision into an SNS topic in the format required by the qldb-ledger-load project.
 */
public class SnsLoaderRevisionVisitor implements RevisionVisitor {

    private final static IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();

    private final String topicArn;
    private final SnsClient snsClient;
    private final boolean useFifo;


    private SnsLoaderRevisionVisitor(SnsClient snsClient, String topicArn) {
        if (snsClient == null)
            throw new IllegalArgumentException("SNS Client cannot be null");

        if (topicArn == null)
            throw new IllegalArgumentException("SNS Topic ARN cannot be null");

        this.snsClient = snsClient;
        this.topicArn = topicArn;
        useFifo = topicArn.endsWith(".fifo");
    }

    public String getTopicArn() {
        return topicArn;
    }

    public SnsClient getSnsClient() {
        return snsClient;
    }

    @Override
    public void setup() {

    }

    @Override
    public void visit(IonStruct revision, String tableName) {
        LoadEvent event = LoadEvent.fromCommittedRevision(revision, tableName);

        PublishRequest.Builder builder = PublishRequest.builder();
        builder.topicArn(this.topicArn);
        builder.message(event.toPrettyString());

        if (useFifo) {
            builder.messageGroupId(event.getId().toString());
        }

        snsClient.publish(builder.build());
    }

    @Override
    public void teardown() {
    }

    public static SnsLoaderRevisionVisitorBuilder builder() {
        return new SnsLoaderRevisionVisitorBuilder();
    }


    public static class SnsLoaderRevisionVisitorBuilder {
        private String topicArn;
        private SnsClient snsClient;

        public static SnsLoaderRevisionVisitorBuilder builder() {
            return new SnsLoaderRevisionVisitorBuilder();
        }

        public SnsLoaderRevisionVisitorBuilder topicArn(String topicArn) {
            this.topicArn = topicArn;
            return this;
        }

        public SnsLoaderRevisionVisitorBuilder snsClient(SnsClient snsClient) {
            this.snsClient = snsClient;
            return this;
        }

        public SnsLoaderRevisionVisitor build() {
            return new SnsLoaderRevisionVisitor(snsClient, topicArn);
        }
    }
}
