package xme.common.kfkprocessor.requestreply.ports;

import java.util.List;

/**
 * Outcome of one committed Cycle: replies that could not be published for per-request reasons. A failure with
 * {@code substituted} true had its fallback undeliverable Error Reply committed in its place.
 */
public record CommitResult(List<ReplyFailure> failures) {

    /** A reply that is not in the destination; its request position was still committed. */
    public record ReplyFailure(String lane, int partition, long position, String correlationId, Reason reason,
            boolean substituted) {

        public ReplyFailure(String lane, int partition, long position, String correlationId, Reason reason) {
            this(lane, partition, position, correlationId, reason, false);
        }
    }

    public enum Reason {
        TOO_LARGE, UNENCODABLE, REJECTED
    }
}
