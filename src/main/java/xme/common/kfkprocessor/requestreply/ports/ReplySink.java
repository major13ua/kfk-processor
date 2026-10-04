package xme.common.kfkprocessor.requestreply.ports;

import java.util.List;

/** Internal port: transactional reply destination. */
public interface ReplySink {

    /**
     * Atomically publishes all {@code replies} and commits the request positions they cover (one Cycle).
     * Either everything is visible or nothing. Throws on failure; the caller retries the same results
     * without re-running Handlers (ADR-0007).
     */
    void commit(List<ReplyRecord> replies);
}
