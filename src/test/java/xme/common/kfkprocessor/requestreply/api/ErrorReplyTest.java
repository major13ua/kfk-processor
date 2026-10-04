package xme.common.kfkprocessor.requestreply.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class ErrorReplyTest {

    @Test
    void errorReplyEchoesCorrelationIdAndRequestKeyUnchanged() {
        ErrorReply<String> reply = ErrorReply.of(ErrorCategory.FAILURE, "corr-1", "key-1");

        assertEquals(ErrorCategory.FAILURE, reply.category());
        assertEquals("corr-1", reply.correlationId());
        assertEquals("key-1", reply.requestKey());
    }

    @Test
    void errorReplyKeepsRequestKeyInstanceUnchanged() {
        Object key = new Object();
        ErrorReply<Object> reply = ErrorReply.of(ErrorCategory.TIMEOUT, "corr-2", key);

        assertSame(key, reply.requestKey());
    }

    @Test
    void errorReplyHoldsOnlyCategoryCorrelationIdAndRequestKey() {
        Set<String> components = Arrays.stream(ErrorReply.class.getRecordComponents())
                .map(c -> c.getName())
                .collect(Collectors.toSet());

        assertEquals(Set.of("category", "correlationId", "requestKey"), components);
    }

    @Test
    void errorReplyToStringCarriesNoFreeText() {
        ErrorReply<String> reply = ErrorReply.of(ErrorCategory.UNDELIVERABLE, "c", "k");

        assertTrue(reply.toString().length() < 200);
    }

    @Test
    void errorCategoriesAreFailureTimeoutUndeliverable() {
        assertEquals(Set.of("FAILURE", "TIMEOUT", "UNDELIVERABLE"),
                Arrays.stream(ErrorCategory.values()).map(Enum::name).collect(Collectors.toSet()));
    }
}
