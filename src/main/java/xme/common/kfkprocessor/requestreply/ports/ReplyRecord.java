package xme.common.kfkprocessor.requestreply.ports;

/**
 * One encoded reply or Error Reply to be committed with its request position. {@code fallback} is the
 * pre-encoded undeliverable Error Reply (correlation id and Request Key only, no payload) that the sink sends
 * in the same transaction instead of {@code value} when that cannot be sent (too large, unencodable, rejected);
 * null when there is no fallback (an Error Reply itself). {@code error} marks Error Replies; the sink puts it on
 * the wire (also for a substituted fallback). {@code correlationIdBytes} and {@code requestKeyBytes} are the
 * request's raw identifier bytes when the identifiers are echoed unchanged; the sink writes them as they are
 * (a String decoded from non-UTF-8 bytes cannot be turned back). Null means: derive from the String values.
 */
public record ReplyRecord(String lane, int partition, long position, String correlationId, Object requestKey,
        byte[] value, boolean error, byte[] fallback, byte[] correlationIdBytes, byte[] requestKeyBytes) {

    /** A record whose identifiers are written from the String values. */
    public ReplyRecord(String lane, int partition, long position, String correlationId, Object requestKey,
            byte[] value, boolean error, byte[] fallback) {
        this(lane, partition, position, correlationId, requestKey, value, error, fallback, null, null);
    }

    /** A record without a fallback. */
    public ReplyRecord(String lane, int partition, long position, String correlationId, Object requestKey,
            byte[] value, boolean error) {
        this(lane, partition, position, correlationId, requestKey, value, error, null);
    }
}
