package xme.common.kfkprocessor.requestreply.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/** Ties intake, Handler execution and commit into Cycles (T13). */
public final class CycleLoop<K, REQ, RES> {

    /** What one iteration did. */
    public enum Iteration { IDLE, PAUSED, COMMITTED, HELD }

    private static final System.Logger LOG = System.getLogger(CycleLoop.class.getName());
    private static final long IDLE_BACKOFF_MILLIS = 10;

    private final Intake intake;
    private final HandlerExecutor<K, REQ, RES> executor;
    private final CommitRetry<K, RES> retry;
    private final WorkerState state;
    private final WorkerMetrics metrics;
    private final Clock clock;
    private final Duration cycleDeadline;
    private final Function<IncomingRequest, Instant> createdAt;
    private final Duration maxPlausibleLag;
    private volatile boolean running;
    private Thread thread;

    public CycleLoop(Intake intake, HandlerExecutor<K, REQ, RES> executor, CommitRetry<K, RES> retry,
            WorkerState state, WorkerMetrics metrics, Clock clock, Duration cycleDeadline,
            Function<IncomingRequest, Instant> createdAt, Duration maxPlausibleLag) {
        this.intake = intake;
        this.executor = executor;
        this.retry = retry;
        this.state = state;
        this.metrics = metrics;
        this.clock = clock;
        this.cycleDeadline = cycleDeadline;
        this.createdAt = createdAt;
        this.maxPlausibleLag = maxPlausibleLag;
    }

    /** One iteration: intake, run, commit; or only {@code retry.tick()} while paused. */
    public Iteration runOnce() {
        if (retry.holding()) {
            return tickPaused();
        }
        IntakeResult in = intake.intake();
        if (in.paused()) {
            return tickPaused();
        }
        if (in.accepted().isEmpty() && in.immediateErrors().isEmpty()) {
            state.setPendingWork(false);
            return Iteration.IDLE;
        }
        Instant start = clock.instant();
        state.setPendingWork(true);
        state.setCycleOpen(true);
        List<HandlerResult<K, RES>> results = new ArrayList<>();
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
            }
        }
        Optional<CommitResult> committed = retry.commit(results);
        metrics.cycleDuration(Duration.between(start, clock.instant()));
        if (committed.isEmpty()) {
            return Iteration.HELD;
        }
        state.setPendingWork(false);
        recordLag(results, committed.get());
        return Iteration.COMMITTED;
    }

    private Iteration tickPaused() {
        try {
            intake.keepAlive();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Keep-alive poll failed: " + e.getClass().getName());
        }
        Optional<CommitResult> out = retry.tick();
        if (out.isPresent()) {
            state.setPendingWork(false);
            return Iteration.COMMITTED;
        }
        return Iteration.PAUSED;
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
    }

    public void stop() {
        Thread t;
        synchronized (this) {
            running = false;
            t = thread;
            thread = null;
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
