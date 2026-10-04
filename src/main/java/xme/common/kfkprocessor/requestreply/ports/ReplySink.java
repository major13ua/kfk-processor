package xme.common.kfkprocessor.requestreply.ports;

import java.util.List;

/** Internal port: transactional reply destination. */
public interface ReplySink {

    /**
     * Atomically publishes all {@code replies} and commits the request positions they cover (one Cycle).
     * Either everything is visible or nothing. A failure specific to one reply (too large, cannot be encoded,
     * rejected on send) does not abort the transaction: it is reported in the {@link CommitResult} and the
     * request position still commits. When that reply carries a {@link ReplyRecord#fallback()}, the sink sends the
     * fallback instead (same transaction, same headers) and lists the failure with {@code substituted} true;
     * without a fallback, or when the fallback fails too, the failure is only reported. Throws {@link ReplyDestinationFault} when the destination is unavailable or
     * denies access (nothing committed), and another runtime exception when the commit itself fails; the caller
     * retries the same results without re-running Handlers (ADR-0007).
     */
    CommitResult commit(List<ReplyRecord> replies);
}
