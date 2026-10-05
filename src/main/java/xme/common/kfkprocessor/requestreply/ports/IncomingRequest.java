package xme.common.kfkprocessor.requestreply.ports;

import java.util.Map;

/**
 * A request fetched from a lane, independent of the messaging platform. {@code assignment} identifies the lane
 * partition's assignment the request was fetched under (opaque to the engine, see
 * {@link RequestLanes#revokedSinceFetch}).
 */
public record IncomingRequest(
        String lane,
        int partition,
        long position,
        Object requestKey,
        String correlationId,
        Map<String, byte[]> headers,
        byte[] payload,
        long assignment) {

    /** A request of the first assignment (adapters without rebalancing, tests). */
    public IncomingRequest(String lane, int partition, long position, Object requestKey, String correlationId,
            Map<String, byte[]> headers, byte[] payload) {
        this(lane, partition, position, requestKey, correlationId, headers, payload, 0L);
    }

    /** Header (record header on Kafka) carrying the correlation id as the Requester sent it, raw bytes. */
    public static final String CORRELATION_ID = "correlation_id";
    /** Header carrying the Request Key as the Requester sent it, raw bytes. */
    public static final String REQUEST_KEY = "request_key";
}
