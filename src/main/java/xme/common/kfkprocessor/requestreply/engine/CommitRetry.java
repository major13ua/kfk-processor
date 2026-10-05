package xme.common.kfkprocessor.requestreply.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * Retries a failed Cycle commit with the same results, and on exhaustion or destination fault pauses the worker,
 * holds the results in memory and re-commits them after a probe succeeds (T12). Handlers are never re-run.
 */
public final class CommitRetry<K, RES> {

    /** Operator alert: why the worker paused (null: a fault without pause) and the fault id. */
    public record Alert(WorkerState.PauseReason reason, String faultId) {
        /** A fault that does not pause the worker: no pause reason. */
        public static Alert fault(String faultId) {
            return new Alert(null, faultId);
        }
    }

    private static final String UNAVAILABLE = "request_reply.reply_destination.unavailable";
    private static final String PERMISSION_DENIED = "request_reply.reply_destination.permission_denied";
    private static final String FENCED = "request_reply.reply_destination.fenced";

    private final CycleCommitter<K, RES> committer;
    private final DestinationProbe probe;
    private final WorkerState state;
    private final Clock clock;
    private final Duration probeInterval;
    private final int attempts;
    private final Consumer<Alert> alert;
    private final WorkerMetrics metrics;
    private List<HandlerResult<K, RES>> held;
    private WorkerState.PauseReason heldReason;
    private Instant lastProbe;

    public CommitRetry(CycleCommitter<K, RES> committer, DestinationProbe probe, WorkerState state, Clock clock,
            Duration probeInterval, int attempts, Consumer<Alert> alert) {
        this(committer, probe, state, clock, probeInterval, attempts, alert, null);
    }

    public CommitRetry(CycleCommitter<K, RES> committer, DestinationProbe probe, WorkerState state, Clock clock,
            Duration probeInterval, int attempts, Consumer<Alert> alert, WorkerMetrics metrics) {
        this.metrics = metrics;
        this.committer = committer;
        this.probe = probe;
        this.state = state;
        this.clock = clock;
        this.probeInterval = probeInterval;
        this.attempts = Math.max(1, attempts);
        this.alert = alert;
    }

    /** Empty when the results are now held (worker paused); otherwise the committed Cycle's result. */
    public synchronized Optional<CommitResult> commit(List<HandlerResult<K, RES>> results) {
        for (int i = 0; i < attempts; i++) {
            try {
                commitAttempted();
                CommitResult out = committer.commit(results);
                state.commitSucceeded();
                return Optional.of(out);
            } catch (ReplyDestinationFault fault) {
                abandonIfInterrupted(fault);
                hold(results, fault);
                return Optional.empty();
            } catch (RuntimeException ex) {
                abandonIfInterrupted(ex);
                // same results, next attempt
            }
        }
        hold(results, null);
        return Optional.empty();
    }

    /** The results held awaiting the destination, or null. */
    public synchronized List<HandlerResult<K, RES>> heldResults() {
        return held;
    }

    /**
     * Removes held results matching {@code stale} (their partition was revoked while held). A Cycle left empty
     * stays held until the probe passes, then resumes without a commit.
     */
    public synchronized void dropHeld(java.util.function.Predicate<HandlerResult<K, RES>> stale) {
        if (held != null && held.stream().anyMatch(stale)) {
            held = held.stream().filter(stale.negate()).toList();
        }
    }

    /** An interrupt is a graceful stop, not a destination fault: leave the Cycle uncommitted, no pause, no alert. */
    private static void abandonIfInterrupted(RuntimeException cause) {
        if (Thread.currentThread().isInterrupted()) {
            throw new CycleInterruptedException(cause);
        }
    }

    private void commitAttempted() {
        if (metrics != null) {
            metrics.commitAttempt();
        }
    }

    /** True while results are held awaiting the destination. */
    public synchronized boolean holding() {
        return held != null;
    }

    /** Probes when holding and the probe interval elapsed; on success re-commits the held results and resumes. */
    public synchronized Optional<CommitResult> tick() {
        if (held == null) {
            return Optional.empty();
        }
        Instant now = clock.instant();
        if (Duration.between(lastProbe, now).compareTo(probeInterval) < 0) {
            return Optional.empty();
        }
        lastProbe = now;
        try {
            probe.probe();
            CommitResult out = new CommitResult(List.of());
            if (!held.isEmpty()) {
                commitAttempted();
                out = committer.commit(held);
            }
            state.resume(heldReason);
            state.commitSucceeded();
            held = null;
            heldReason = null;
            return Optional.of(out);
        } catch (ReplyDestinationFault fault) {
            abandonIfInterrupted(fault);
            moveReason(reasonOf(fault));
        } catch (RuntimeException ex) {
            abandonIfInterrupted(ex);
            // still failing: keep holding under the current reason
        }
        return Optional.empty();
    }

    private void hold(List<HandlerResult<K, RES>> results, ReplyDestinationFault fault) {
        held = results;
        lastProbe = clock.instant();
        WorkerState.PauseReason reason = fault == null ? WorkerState.PauseReason.DESTINATION : reasonOf(fault);
        heldReason = reason;
        state.pause(reason);
        alert.accept(new Alert(reason, faultId(fault)));
    }

    private static String faultId(ReplyDestinationFault fault) {
        if (fault instanceof ReplyDestinationFault.PermissionDenied) {
            return PERMISSION_DENIED;
        }
        return fault instanceof ReplyDestinationFault.Fenced ? FENCED : UNAVAILABLE;
    }

    private void moveReason(WorkerState.PauseReason next) {
        if (next != heldReason) {
            state.pause(next);
            state.resume(heldReason);
            heldReason = next;
        }
    }

    private static WorkerState.PauseReason reasonOf(ReplyDestinationFault fault) {
        return fault instanceof ReplyDestinationFault.PermissionDenied
                ? WorkerState.PauseReason.PERMISSION : WorkerState.PauseReason.DESTINATION;
    }
}
