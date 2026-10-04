package xme.common.kfkprocessor.requestreply.api;

/**
 * The single business callback of a request-reply worker. At-least-once: dedupe on
 * {@link RequestContext#idempotencyKey()}.
 */
@FunctionalInterface
public interface RequestReplyHandler<K, REQ, RES> {

    /** One request in, one reply out. Any thrown exception becomes an Error Reply (failure). */
    RES handle(RequestContext<K> context, REQ request) throws Exception;
}
