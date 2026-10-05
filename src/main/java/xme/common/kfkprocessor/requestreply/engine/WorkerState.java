package xme.common.kfkprocessor.requestreply.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * Worker state machine: running, idle, paused(reason), stalled (AC-17). Thread-safe; all access is
 * synchronized. Pause takes precedence over stall; several pause reasons may be active at once.
 */
public final class WorkerState {

    public enum Status { RUNNING, IDLE, PAUSED, STALLED }

    public enum PauseReason { LIMITER, DESTINATION, PERMISSION }

    private final Clock clock;
    private final Duration stallThreshold;
    private final WorkerMetrics metrics;
    private final Set<PauseReason> pauses = new LinkedHashSet<>();
    private Instant lastCommit;
    private boolean pendingWork;
    private boolean cycleOpen;
    private WorkerMetrics.State published;

    public WorkerState(Clock clock, Duration stallThreshold, WorkerMetrics metrics) {
        this.clock = clock;
        this.stallThreshold = stallThreshold;
        this.metrics = metrics;
        this.lastCommit = clock.instant();
    }

    public synchronized void setPendingWork(boolean pending) {
        startStallClockIfLeavingIdle(pending);
        this.pendingWork = pending;
        evaluate();
    }

    public synchronized void setCycleOpen(boolean open) {
        startStallClockIfLeavingIdle(open);
        this.cycleOpen = open;
        evaluate();
    }

    public Duration stallThreshold() {
        return stallThreshold;
    }

    /** Idle time is not stall time: the clock restarts when work arrives on an idle, unpaused worker. */
    private void startStallClockIfLeavingIdle(boolean becomingBusy) {
        if (becomingBusy && !pendingWork && !cycleOpen && pauses.isEmpty()) {
            lastCommit = clock.instant();
        }
    }

    public synchronized void pause(PauseReason reason) {
        pauses.add(reason);
        evaluate();
    }

    /** Removes only the given reason; the stall clock restarts when the last reason is removed. */
    public synchronized void resume(PauseReason reason) {
        if (pauses.remove(reason) && pauses.isEmpty()) {
            lastCommit = clock.instant();
        }
        evaluate();
    }

    public synchronized void commitSucceeded() {
        lastCommit = clock.instant();
        evaluate();
    }

    /** Re-evaluates against the clock, publishes a change if any, returns the current status. */
    public synchronized Status evaluate() {
        Status status = compute();
        WorkerMetrics.State mapped = switch (status) {
            case PAUSED -> WorkerMetrics.State.PAUSED;
            case STALLED -> WorkerMetrics.State.STALLED;
            case RUNNING, IDLE -> WorkerMetrics.State.RUNNING;
        };
        if (mapped != published) {
            published = mapped;
            metrics.state(mapped);
        }
        return status;
    }

    public synchronized PauseReason pauseReason() {
        return pauses.isEmpty() ? null : pauses.iterator().next();
    }

    private Status compute() {
        if (!pauses.isEmpty()) {
            return Status.PAUSED;
        }
        if (!pendingWork && !cycleOpen) {
            return Status.IDLE;
        }
        boolean stalled = Duration.between(lastCommit, clock.instant()).compareTo(stallThreshold) > 0;
        return stalled ? Status.STALLED : Status.RUNNING;
    }
}
