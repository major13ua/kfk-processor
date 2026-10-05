package xme.common.kfkprocessor.requestreply.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.GroupMembershipChanged;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.LaneAccessDeniedException;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/** Ties intake, Handler execution and commit into Cycles (T13). */
public final class CycleLoop<K, REQ, RES> {

    /** What one iteration did. */
    public enum Iteration { IDLE, PAUSED, COMMITTED, HELD }

    private static final System.Logger LOG = System.getLogger(CycleLoop.class.getName());
    private static final long IDLE_BACKOFF_MILLIS = 10;
    private static final String LANE_PERMISSION_DENIED = "request_reply.request_lane.permission_denied";

    private static final String CYCLE_FAILED = "request_reply.cycle.failed";
    private static final String REPLY_UNDELIVERABLE = "request_reply.reply.undeliverable";

    private final Intake intake;
    private final HandlerExecutor<K, REQ, RES> executor;
    private final CommitRetry<K, RES> retry;
    private final WorkerState state;
    private final WorkerMetrics metrics;
    private final Clock clock;
    private final Duration cycleDeadline;
    private final Function<IncomingRequest, Instant> createdAt;
    private final Duration maxPlausibleLag;
    private final Duration probeInterval;
    private final Consumer<CommitRetry.Alert> alert;
    /** Set while the lanes cannot be read; retried every probe interval, alerted once per outage. */
    private boolean lanesDenied;
    private Instant lastLaneAttempt;
    private volatile boolean running;
    private Thread thread;
    private Thread watchdog;

    public CycleLoop(Intake intake, HandlerExecutor<K, REQ, RES> executor, CommitRetry<K, RES> retry,
            WorkerState state, WorkerMetrics metrics, Clock clock, Duration cycleDeadline,
            Function<IncomingRequest, Instant> createdAt, Duration maxPlausibleLag) {
        this(intake, executor, retry, state, metrics, clock, cycleDeadline, createdAt, maxPlausibleLag,
                Duration.ofSeconds(5), alert -> { });
    }

    public CycleLoop(Intake intake, HandlerExecutor<K, REQ, RES> executor, CommitRetry<K, RES> retry,
            WorkerState state, WorkerMetrics metrics, Clock clock, Duration cycleDeadline,
            Function<IncomingRequest, Instant> createdAt, Duration maxPlausibleLag, Duration probeInterval,
            Consumer<CommitRetry.Alert> alert) {
        this.intake = intake;
        this.executor = executor;
        this.retry = retry;
        this.state = state;
        this.metrics = metrics;
        this.clock = clock;
        this.cycleDeadline = cycleDeadline;
        this.createdAt = createdAt;
        this.maxPlausibleLag = maxPlausibleLag;
        this.probeInterval = probeInterval;
        this.alert = alert;
    }

    /** One iteration: intake, run, commit; or only {@code retry.tick()} while paused. */
    public Iteration runOnce() {
        if (retry.holding()) {
            return tickPaused();
        }
        if (lanesDenied && Duration.between(lastLaneAttempt, clock.instant()).compareTo(probeInterval) < 0) {
            return tickLanesDenied();
        }
        IntakeResult in;
        try {
            in = intake.intake();
        } catch (LaneAccessDeniedException e) {
            return onLanesDenied();
        }
        if (lanesDenied) {
            lanesDenied = false;
            state.resume(WorkerState.PauseReason.LANE_PERMISSION);
        }
        if (in.paused()) {
            return tickPaused();
        }
        if (in.accepted().isEmpty() && in.immediateErrors().isEmpty()) {
            if (in.available()) {
                // backlog but no allowance: not idle; poll (consuming nothing) so max.poll.interval is not exceeded
                state.setPendingWork(true);
                try {
                    intake.keepAlive();
                } catch (LaneAccessDeniedException e) {
                    return onLanesDenied();
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "Keep-alive poll failed: " + e.getClass().getName());
                }
                return Iteration.IDLE;
            }
            state.setPendingWork(false);
            return Iteration.IDLE;
        }
        List<HandlerResult<K, RES>> results = new ArrayList<>();
        Instant start = clock.instant();
        try {
            runCycle(in, results);
        } catch (CycleInterruptedException e) {
            throw e; // graceful stop: the Cycle's requests stay uncommitted and are redelivered
        } catch (RuntimeException | Error e) {
            abandon(in, e);
            throw e;
        }
        Optional<CommitResult> committed;
        try {
            committed = retry.commit(results);
        } catch (GroupMembershipChanged e) {
            abandon(in, e); // repeated rebalance rejections: serve the requests again
            throw e;
        }
        metrics.cycleDuration(Duration.between(start, clock.instant()));
        if (committed.isEmpty()) {
            return Iteration.HELD;
        }
        state.setPendingWork(false);
        reportUndeliverable(committed.get());
        recordLag(results, committed.get());
        return Iteration.COMMITTED;
    }

    /** Before the commit takes ownership: hand the fetched requests back so they are served again, alert once. */
    private void abandon(IntakeResult in, Throwable cause) {
        try {
            intake.abandon(in);
        } catch (RuntimeException | Error e) {
            cause.addSuppressed(e);
        }
        try {
            alert.accept(CommitRetry.Alert.fault(CYCLE_FAILED));
        } catch (RuntimeException | Error e) {
            cause.addSuppressed(e);
        }
    }

    private void runCycle(IntakeResult in, List<HandlerResult<K, RES>> results) {
        state.setPendingWork(true);
        state.setCycleOpen(true);
        try {
            if (!in.accepted().isEmpty()) {
                in.accepted().stream().map(IncomingRequest::lane).distinct().forEach(lane -> metrics.accepted(lane,
                        in.accepted().stream().filter(r -> r.lane().equals(lane)).count()));
                results.addAll(executor.executeCycle(in.accepted(), cycleDeadline));
            }
            for (IntakeResult.ImmediateError e : in.immediateErrors()) {
                @SuppressWarnings("unchecked")
                ErrorReply<K> reply = (ErrorReply<K>) e.reply();
                results.add(new HandlerResult<>(e.request(), null, reply));
            }
        } finally {
            state.setCycleOpen(false);
        }
        for (HandlerResult<K, RES> r : results) {
            if (r.errorReply() != null) {
                metrics.errorReply(r.errorReply().category());
                if (r.errorReply().category() == ErrorCategory.TIMEOUT) {
                    metrics.handlerTimeout();
                }
            }
        }
    }

    private Iteration onLanesDenied() {
        lastLaneAttempt = clock.instant();
        if (!lanesDenied) {
            lanesDenied = true;
            state.pause(WorkerState.PauseReason.LANE_PERMISSION);
            alert.accept(new CommitRetry.Alert(WorkerState.PauseReason.LANE_PERMISSION, LANE_PERMISSION_DENIED));
            LOG.log(System.Logger.Level.ERROR, "Request lanes cannot be read (" + LANE_PERMISSION_DENIED
                    + "): worker paused, retrying every " + probeInterval);
        }
        return Iteration.PAUSED;
    }

    /** Between retries: keep the group membership alive; the denial it hits again is already reported. */
    private Iteration tickLanesDenied() {
        try {
            intake.keepAlive();
        } catch (LaneAccessDeniedException ignored) {
            // still denied, already alerted
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Keep-alive poll failed: " + e.getClass().getName());
        }
        return Iteration.PAUSED;
    }

    private Iteration tickPaused() {
        try {
            intake.keepAlive();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Keep-alive poll failed: " + e.getClass().getName());
        }
        // the keep-alive poll may have rebalanced: results of revoked partitions are re-read from the committed
        // offset or answered by their new owner, so they must not be committed (live group metadata would accept it)
        retry.dropHeld(r -> intake.revokedSinceFetch(r.request()));
        List<HandlerResult<K, RES>> held = retry.heldResults();
        Optional<CommitResult> out = retry.tick();
        if (out.isPresent()) {
            state.setPendingWork(false);
            if (held != null && !held.isEmpty()) {
                intake.committedAfterHold(held.stream().map(HandlerResult::request).toList());
            }
            reportUndeliverable(out.get());
            recordLag(held == null ? List.of() : held, out.get());
            return Iteration.COMMITTED;
        }
        return Iteration.PAUSED;
    }

    /**
     * Replies the sink replaced by their undeliverable Error Reply or could not deliver at all are counted as
     * UNDELIVERABLE; a reply without any deliverable fallback is also logged (no payload) and alerted once per commit.
     */
    private void reportUndeliverable(CommitResult commit) {
        long dropped = 0;
        for (CommitResult.ReplyFailure f : commit.failures()) {
            metrics.errorReply(ErrorCategory.UNDELIVERABLE);
            if (!f.substituted()) {
                dropped++;
            }
        }
        if (dropped > 0) {
            LOG.log(System.Logger.Level.ERROR, dropped + " reply(ies) have no deliverable fallback and were dropped ("
                    + REPLY_UNDELIVERABLE + ")");
            alert.accept(new CommitRetry.Alert(WorkerState.PauseReason.DESTINATION, REPLY_UNDELIVERABLE));
        }
    }

    private void recordLag(List<HandlerResult<K, RES>> results, CommitResult commit) {
        Set<String> undelivered = new HashSet<>();
        for (CommitResult.ReplyFailure f : commit.failures()) {
            if (!f.substituted()) {
                undelivered.add(f.correlationId());
            }
        }
        Instant now = clock.instant();
        for (HandlerResult<K, RES> r : results) {
            IncomingRequest q = r.request();
            if (undelivered.contains(q.correlationId())) {
                continue;
            }
            Duration lag = Duration.between(createdAt.apply(q), now);
            boolean implausible = lag.isNegative() || lag.compareTo(maxPlausibleLag) > 0;
            metrics.consistencyLag(q.lane(), lag, implausible);
        }
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(this::loop, "request-reply-cycle-loop");
        thread.setDaemon(true);
        thread.start();
        watchdog = new Thread(this::watch, "request-reply-stall-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();
    }

    /** Re-evaluates the stall state on the clock, so a stuck or blocked worker is flagged without a state change. */
    private void watch() {
        long intervalMillis = Math.max(10, Math.min(1_000, state.stallThreshold().toMillis() / 4));
        while (running) {
            state.evaluate();
            try {
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    public void stop() {
        Thread t;
        synchronized (this) {
            running = false;
            t = thread;
            thread = null;
            if (watchdog != null) {
                watchdog.interrupt();
                watchdog = null;
            }
        }
        if (t != null) {
            t.interrupt();
            try {
                t.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void loop() {
        while (running) {
            Iteration it;
            try {
                it = runOnce();
            } catch (CycleInterruptedException e) {
                return; // graceful stop: the Cycle's requests stay uncommitted and are redelivered
            } catch (RuntimeException | Error e) {
                if (!running || Thread.currentThread().isInterrupted()) {
                    return;
                }
                LOG.log(System.Logger.Level.ERROR, "Cycle iteration failed: " + e.getClass().getName());
                it = Iteration.IDLE;
            }
            if (it == Iteration.IDLE || it == Iteration.PAUSED || it == Iteration.HELD) {
                try {
                    Thread.sleep(IDLE_BACKOFF_MILLIS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}
