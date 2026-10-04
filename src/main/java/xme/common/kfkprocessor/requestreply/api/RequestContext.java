package xme.common.kfkprocessor.requestreply.api;

import java.util.Map;

/** Per-request context handed to the Handler. Request key and correlation id are echoed unchanged on the reply. */
public record RequestContext<K>(
        K requestKey,
        String correlationId,
        String idempotencyKey,
        String lane,
        Map<String, byte[]> headers,
        CancellationSignal cancellation) {
}
