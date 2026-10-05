package xme.common.kfkprocessor.requestreply.ports;

import java.util.Map;

/** A request fetched from a lane, independent of the messaging platform. */
public record IncomingRequest(
        String lane,
        int partition,
        long position,
        Object requestKey,
        String correlationId,
        Map<String, byte[]> headers,
        byte[] payload) {

    /** Header (record header on Kafka) carrying the correlation id as the Requester sent it, raw bytes. */
    public static final String CORRELATION_ID = "correlation_id";
    /** Header carrying the Request Key as the Requester sent it, raw bytes. */
    public static final String REQUEST_KEY = "request_key";
}
