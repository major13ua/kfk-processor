package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.AllowanceStoreUnavailableException;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.PauseReason;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.Status;
import xme.common.kfkprocessor.requestreply.ports.AllowanceStore;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/** AC-01 with fakes: intake, Handler, commit, metrics and WorkerState wired through CycleLoop.runOnce(). */
class CycleLoopTest {

    private static final long MAX_BYTES = 100;
    private static final Duration PROBE = Duration.ofSeconds(5);
    private static final Duration MAX_LAG = Duration.ofDays(1);

    private static final class FakeClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        @Override public Instant instant() { return now; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId z) { return this; }
    }

    private static final class FakeStore implements AllowanceStore {
        boolean down;
        int reserves;
        @Override public long reserve(long units) {
            reserves++;
            if (down) {
                throw new AllowanceStoreUnavailableException("down");
            }
            return units;
        }
        @Override public void giveBack(long units) { }
    }

    private static final class FakeLanes implements RequestLanes {
        final List<IncomingRequest> waiting = new ArrayList<>();
        int fetches;
        @Override public List<IncomingRequest> fetch(Map<String, Integer> quotaByLane) {
            fetches++;
            List<IncomingRequest> out = new ArrayList<>(waiting);
            waiting.clear();
            return out;
        }
        @Override public void pause() { }
        @Override public void resume() { }
    }

    private static final class FakeSink implements ReplySink {
        final List<List<ReplyRecord>> commits = new ArrayList<>();
        RuntimeException failWith;
        @Override public CommitResult commit(List<ReplyRecord> replies) {
            if (failWith != null) {
                throw failWith;
            }
            commits.add(replies);
            return new CommitResult(List.of());
        }
    }

    private static final class FakeProbe implements DestinationProbe {
        int calls;
        @Override public void probe() { calls++; }
    }

    private static final class RecordingMetrics implements WorkerMetrics {
        final Map<String, Long> accepted = new LinkedHashMap<>();
        final List<Object[]> lags = new ArrayList<>();
        final List<ErrorCategory> errorReplies = new ArrayList<>();
        final List<Duration> cycleDurations = new ArrayList<>();
        @Override public void accepted(String lane, long count) { accepted.merge(lane, count, Long::sum); }
        @Override public void consistencyLag(String lane, Duration lag, boolean implausible) {
            lags.add(new Object[] {lane, lag, implausible});
        }
        @Override public void state(State state) { }
        @Override public void errorReply(ErrorCategory category) { errorReplies.add(category); }
        @Override public void handlerTimeout() { }
        @Override public void commitAttempt() { }
        @Override public void cycleDuration(Duration duration) { cycleDurations.add(duration); }
        @Override public void groupMembershipChange() { }
    }

    private FakeClock clock;
    private FakeStore store;
    private FakeLanes lanes;
    private FakeSink sink;
    private FakeProbe probe;
    private RecordingMetrics metrics;
    private WorkerState state;
    private CommitRetry<String, String> retry;
    private Map<String, Instant> createdAt;
    private RequestReplyHandler<String, String, String> handler;
    private long seq;

    @BeforeEach
    void setUp() {
        clock = new FakeClock();
        store = new FakeStore();
        lanes = new FakeLanes();
        sink = new FakeSink();
        probe = new FakeProbe();
        metrics = new RecordingMetrics();
        createdAt = new LinkedHashMap<>();
        handler = (ctx, req) -> "re:" + req;
        state = new WorkerState(clock, Duration.ofSeconds(60), metrics);
        CycleCommitter<String, String> committer = new CycleCommitter<>(sink,
                r -> ("reply|" + r.correlationId() + "|" + r.data()).getBytes(StandardCharsets.UTF_8),
                e -> ("error|" + e.category()).getBytes(StandardCharsets.UTF_8));
        retry = new CommitRetry<>(committer, probe, state, clock, PROBE, 2, a -> { });
    }

    private CycleLoop<String, String, String> loop(Duration cycleDeadline) {
        Map<String, Double> weights = new LinkedHashMap<>();
        weights.put("high", 3.0);
        weights.put("low", 1.0);
        Intake intake = new Intake(store, lanes, state, clock, weights, 0.05, MAX_BYTES, 10, PROBE);
        HandlerExecutor<String, String, String> executor = new HandlerExecutor<>(
                (ctx, req) -> handler.handle(ctx, req),
                r -> new String(r.payload(), StandardCharsets.UTF_8),
                Duration.ofSeconds(30));
        return new CycleLoop<>(intake, executor, retry, state, metrics, clock, cycleDeadline,
                r -> createdAt.getOrDefault(r.correlationId(), clock.instant()), MAX_LAG);
    }

    private IncomingRequest req(String lane, int payloadBytes) {
        long n = seq++;
        return new IncomingRequest(lane, 0, n, "key-" + n, "corr-" + n, Map.of(), new byte[payloadBytes]);
    }

    private IncomingRequest reqText(String lane, String text) {
        long n = seq++;
        return new IncomingRequest(lane, 0, n, "key-" + n, "corr-" + n, Map.of(),
                text.getBytes(StandardCharsets.UTF_8));
    }

    // AC-01
    @Test
    void requestInProducesReplyWithSameCorrelationIdAndRequestKey() {
        IncomingRequest q = reqText("high", "hello");
        lanes.waiting.add(q);

        CycleLoop.Iteration it = loop(Duration.ofSeconds(5)).runOnce();

        assertEquals(CycleLoop.Iteration.COMMITTED, it);
        assertEquals(1, sink.commits.size());
        ReplyRecord r = sink.commits.get(0).get(0);
        assertEquals(q.correlationId(), r.correlationId());
        assertEquals(q.requestKey(), r.requestKey());
        assertFalse(r.error());
        assertEquals("reply|" + q.correlationId() + "|re:hello", new String(r.value(), StandardCharsets.UTF_8));
        assertEquals(q.position(), r.position());
    }

    @Test
    void intakeImmediateErrorRepliesAreCommittedWithTheirPositions() {
        IncomingRequest good = reqText("high", "ok");
        IncomingRequest oversized = req("high", 200);
        lanes.waiting.add(good);
        lanes.waiting.add(oversized);

        loop(Duration.ofSeconds(5)).runOnce();

        assertEquals(1, sink.commits.size());
        List<ReplyRecord> recs = sink.commits.get(0);
        assertEquals(2, recs.size());
        ReplyRecord err = recs.stream().filter(ReplyRecord::error).findFirst().orElseThrow();
        assertEquals(oversized.position(), err.position());
        assertEquals(oversized.correlationId(), err.correlationId());
        assertTrue(metrics.errorReplies.contains(ErrorCategory.FAILURE));
    }

    @Test
    void nothingPendingIsAnIdleIterationWithoutCommit() {
        CycleLoop.Iteration it = loop(Duration.ofSeconds(5)).runOnce();

        assertEquals(CycleLoop.Iteration.IDLE, it);
        assertTrue(sink.commits.isEmpty());
        assertEquals(Status.IDLE, state.evaluate());
    }

    @Test
    void whilePausedSkipsIntakeButTicksCommitRetryUntilHeldResultsCommit() {
        lanes.waiting.add(reqText("high", "x"));
        sink.failWith = new ReplyDestinationFault.Unavailable("down", null);
        CycleLoop<String, String, String> loop = loop(Duration.ofSeconds(5));
        assertEquals(CycleLoop.Iteration.HELD, loop.runOnce());
        assertEquals(PauseReason.DESTINATION, state.pauseReason());
        int reservesBefore = store.reserves;
        int fetchesBefore = lanes.fetches;
        lanes.waiting.add(reqText("high", "y"));

        // still inside the probe interval: no intake, no probe
        assertEquals(CycleLoop.Iteration.PAUSED, loop.runOnce());
        assertEquals(0, probe.calls);

        // destination recovers, probe interval elapsed: the held Cycle commits, intake still skipped this round
        sink.failWith = null;
        clock.advance(PROBE.plusSeconds(1));
        loop.runOnce();

        assertEquals(1, probe.calls);
        assertEquals(1, sink.commits.size());
        assertEquals(reservesBefore, store.reserves, "no intake while paused");
        assertEquals(fetchesBefore, lanes.fetches);
        assertEquals(null, state.pauseReason());
    }

    @Test
    void limiterPauseSkipsFetchAndDoesNotCommit() {
        store.down = true;
        lanes.waiting.add(reqText("high", "x"));

        CycleLoop.Iteration it = loop(Duration.ofSeconds(5)).runOnce();

        assertEquals(CycleLoop.Iteration.PAUSED, it);
        assertEquals(0, lanes.fetches);
        assertTrue(sink.commits.isEmpty());
    }

    @Test
    void workerStateTracksOpenCycleAndPendingWorkAndCommitTime() throws Exception {
        List<Status> seenDuringHandler = new ArrayList<>();
        handler = (ctx, req) -> {
            seenDuringHandler.add(state.evaluate());
            return "r";
        };
        lanes.waiting.add(reqText("high", "x"));
        clock.advance(Duration.ofSeconds(50));
        CycleLoop<String, String, String> loop = loop(Duration.ofSeconds(5));

        loop.runOnce();
        assertEquals(List.of(Status.RUNNING), seenDuringHandler, "Cycle open while the Handler runs");
        assertEquals(Status.IDLE, state.evaluate(), "closed, nothing pending after commit");

        // commitSucceeded restarted the stall clock: 50s later plus 20s must not be stalled
        clock.advance(Duration.ofSeconds(20));
        state.setPendingWork(true);
        assertEquals(Status.RUNNING, state.evaluate());
    }

    @Test
    void cycleEndsAtTheCycleDeadlineAndCommitsTimeoutErrorReplies() {
        handler = (ctx, req) -> {
            Thread.sleep(10_000);
            return "late";
        };
        lanes.waiting.add(reqText("high", "slow"));
        long t0 = System.nanoTime();

        CycleLoop.Iteration it = loop(Duration.ofMillis(200)).runOnce();

        assertTrue(Duration.ofNanos(System.nanoTime() - t0).compareTo(Duration.ofSeconds(3)) < 0);
        assertEquals(CycleLoop.Iteration.COMMITTED, it);
        ReplyRecord r = sink.commits.get(0).get(0);
        assertTrue(r.error());
        assertEquals("error|TIMEOUT", new String(r.value(), StandardCharsets.UTF_8));
        assertEquals(1, metrics.cycleDurations.size());
    }

    @Test
    void recordsConsistencyLagOncePerCommittedReplyAndFlagsImplausible() {
        IncomingRequest ok = reqText("high", "a");
        IncomingRequest future = reqText("high", "b");
        IncomingRequest ancient = reqText("low", "c");
        createdAt.put(ok.correlationId(), clock.instant().minusSeconds(5));
        createdAt.put(future.correlationId(), clock.instant().plusSeconds(10));
        createdAt.put(ancient.correlationId(), clock.instant().minus(Duration.ofDays(30)));
        lanes.waiting.addAll(List.of(ok, future, ancient));

        loop(Duration.ofSeconds(5)).runOnce();

        assertEquals(3, metrics.lags.size(), "once per committed reply");
        Object[] a = lagFor("high", Duration.ofSeconds(5));
        assertEquals(false, a[2]);
        Object[] f = lagFor("high", Duration.ofSeconds(-10));
        assertEquals(true, f[2], "negative lag is implausible");
        Object[] old = lagFor("low", Duration.ofDays(30));
        assertEquals(true, old[2], "huge lag is implausible");
    }

    private Object[] lagFor(String lane, Duration lag) {
        return metrics.lags.stream()
                .filter(l -> l[0].equals(lane) && l[1].equals(lag))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no lag " + lag + " for lane " + lane));
    }

    @Test
    void recordsAcceptedPerLaneAndErrorReplies() {
        handler = (ctx, req) -> {
            if (req.equals("boom")) {
                throw new IllegalStateException("x");
            }
            return "r";
        };
        lanes.waiting.addAll(List.of(reqText("high", "a"), reqText("high", "b"), reqText("low", "boom")));

        loop(Duration.ofSeconds(5)).runOnce();

        assertEquals(2L, metrics.accepted.get("high"));
        assertEquals(1L, metrics.accepted.get("low"));
        assertEquals(List.of(ErrorCategory.FAILURE), metrics.errorReplies);
    }
}
