package xme.common.kfkprocessor.requestreply.ports;

/**
 * One encoded reply or Error Reply to be committed with its request position. {@code fallback} is the
 * pre-encoded undeliverable Error Reply (correlation id and Request Key only, no payload) that the sink sends
 * in the same transaction instead of {@code value} when that cannot be sent (too large, unencodable, rejected);
 * null when there is no fallback (an Error Reply itself).
 */
public record ReplyRecord(String lane, int partition, long position, String correlationId, Object requestKey,
        byte[] value, boolean error, byte[] fallback) {

    /** A record without a fallback. */
    public ReplyRecord(String lane, int partition, long position, String correlationId, Object requestKey,
            byte[] value, boolean error) {
        this(lane, partition, position, correlationId, requestKey, value, error, null);
    }
}
