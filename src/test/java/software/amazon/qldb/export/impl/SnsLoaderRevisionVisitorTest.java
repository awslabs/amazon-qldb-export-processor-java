package software.amazon.qldb.export.impl;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sns.SnsClient;

import static org.junit.jupiter.api.Assertions.*;


public class SnsLoaderRevisionVisitorTest {
    @Test
    public void testNoArgumentsThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SnsLoaderRevisionVisitor.builder().build());
    }

    @Test
    public void testClientArgumentOnlyThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            SnsLoaderRevisionVisitor.builder()
                    .snsClient(SnsClient.builder().build())
                    .build();
        });
    }

    @Test
    public void testTopicArnArgumentOnlyThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            SnsLoaderRevisionVisitor.builder()
                    .topicArn("arn:aws:sns:us-east-1:000000000000:MyTopic")
                    .build();
        });
    }

    @Test
    public void testCorrect() {
        String arn = "arn:aws:sns:us-east-1:000000000000:MyTopic";
        SnsLoaderRevisionVisitor visitor = SnsLoaderRevisionVisitor.builder()
                .topicArn(arn)
                .snsClient(SnsClient.builder().build())
                .build();

        assertEquals(arn, visitor.getTopicArn());
        assertNotNull(visitor.getSnsClient());
    }
}
