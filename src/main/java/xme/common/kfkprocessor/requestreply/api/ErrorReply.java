package xme.common.kfkprocessor.requestreply.api;

/** Error Reply: category, correlation id and Request Key only. Never exception text, stack trace or payload. */
public record ErrorReply<K>(ErrorCategory category, String correlationId, K requestKey) {

    public static <K> ErrorReply<K> of(ErrorCategory category, String correlationId, K requestKey) {
        return new ErrorReply<>(category, correlationId, requestKey);
    }
}
