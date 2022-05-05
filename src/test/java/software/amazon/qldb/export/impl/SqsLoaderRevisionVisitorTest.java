package software.amazon.qldb.export.impl;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;

import static org.junit.jupiter.api.Assertions.*;


public class SqsLoaderRevisionVisitorTest {
    @Test
    public void testNoArgumentsThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SqsLoaderRevisionVisitor.builder().build());
    }

    @Test
    public void testClientArgumentOnlyThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            SqsLoaderRevisionVisitor.builder()
                    .sqsClient(SqsClient.builder().build())
                    .build();
        });
    }

    @Test
    public void testQueueUrlArgumentOnlyThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            SqsLoaderRevisionVisitor.builder()
                    .queueUrl("https://sqs.us-east-1.amazonaws.com/000000000000/my-queue")
                    .build();
        });
    }

    @Test
    public void testCorrect() {
        String url = "https://sqs.us-east-1.amazonaws.com/000000000000/my-queue";
        SqsLoaderRevisionVisitor visitor = SqsLoaderRevisionVisitor.builder()
                .queueUrl(url)
                .sqsClient(SqsClient.builder().build())
                .build();

        assertEquals(url, visitor.getQueueUrl());
        assertNotNull(visitor.getSqsClient());
    }
}
