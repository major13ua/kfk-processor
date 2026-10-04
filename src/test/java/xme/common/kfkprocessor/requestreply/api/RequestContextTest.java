package xme.common.kfkprocessor.requestreply.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;

import org.junit.jupiter.api.Test;

class RequestContextTest {

    @Test
    void contextCarriesIdempotencyKeyAndEchoedIdentifiers() {
        CancellationSignal signal = new CancellationSignal() {
            public boolean isCancelled() { return false; }
            public void throwIfCancelled() { }
        };
        String key = IdempotencyKey.of("gold", 3, 42L);

        RequestContext<String> ctx =
                new RequestContext<>("key-1", "corr-1", key, "gold", Map.of(), signal);

        assertEquals("gold:3:42", ctx.idempotencyKey());
        assertEquals("key-1", ctx.requestKey());
        assertEquals("corr-1", ctx.correlationId());
        assertEquals("gold", ctx.lane());
        assertFalse(ctx.cancellation().isCancelled());
    }
}
