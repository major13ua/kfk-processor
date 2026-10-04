package xme.common.kfkprocessor.requestreply.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ReplyTest {

    @Test
    void replyEchoesCorrelationIdAndRequestKeyUnchanged() {
        Reply<String, String> reply = new Reply<>("corr-1", "key-1", "payload");

        assertEquals("corr-1", reply.correlationId());
        assertEquals("key-1", reply.requestKey());
        assertEquals("payload", reply.data());
    }
}
