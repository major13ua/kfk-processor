package xme.common.kfkprocessor.requestreply.ports;

/**
 * The principal may not read a request lane (topic) or join the consumer group. Nothing was consumed. Fault id
 * {@code request_reply.request_lane.permission_denied}; the engine pauses the worker, alerts and keeps retrying.
 */
public class LaneAccessDeniedException extends RuntimeException {

    public LaneAccessDeniedException(String message, Throwable cause) {
        super(message, cause);
    }
}
