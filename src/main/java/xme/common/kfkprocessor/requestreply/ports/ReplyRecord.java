package xme.common.kfkprocessor.requestreply.ports;

/** One encoded reply or Error Reply to be committed with its request position. */
public record ReplyRecord(String lane, int partition, long position, String correlationId, Object requestKey,
        byte[] value, boolean error) {
}
