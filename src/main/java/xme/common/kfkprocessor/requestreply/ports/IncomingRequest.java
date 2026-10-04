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
}
