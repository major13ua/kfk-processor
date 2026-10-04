package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.Reply;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.PauseReason;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.Status;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/** AC-08b, AC-09: fake sink, fake probe, fake clock; the Handler is a counter that must not move after the first run. */
class CommitRetryTest {

    private static final int ATTEMPTS = 3;
    private static final Duration PROBE_EVERY = Duration.ofSeconds(10);

    private static final class FakeClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        @Override public Instant instant() { return now; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId z) { return this; }
    }

    /** Throws the queued failures in order, then succeeds. */
    private static final class FakeSink implements ReplySink {
        final List<List<ReplyRecord>> calls = new ArrayList<>();
        final List<RuntimeException> failures = new ArrayList<>();
        boolean alwaysFail;
        RuntimeException alwaysFailWith;

        @Override
        public CommitResult commit(List<ReplyRecord> replies) {
            calls.add(replies);
            if (alwaysFail) {
                throw alwaysFailWith;
            }
            if (!failures.isEmpty()) {
                throw failures.remove(0);
            }
            return new CommitResult(List.of());
        }
    }

    private static final class FakeProbe implements DestinationProbe {
        int calls;
        RuntimeException failWith;

        @Override
        public void probe() {
            calls++;
            if (failWith != null) {
                throw failWith;
            }
        }
    }

    private final AtomicInteger handlerCalls = new AtomicInteger();
    private final FakeSink sink = new FakeSink();
    private final FakeProbe probe = new FakeProbe();
    private final List<CommitRetry.Alert> alerts = new ArrayList<>();
    private final List<WorkerMetrics.State> published = new ArrayList<>();
    private FakeClock clock;
    private WorkerState state;
    private CommitRetry<String, String> retry;

    @BeforeEach
    void setUp() {
        clock = new FakeClock();
        WorkerMetrics metrics = (WorkerMetrics) Proxy.newProxyInstance(
                WorkerMetrics.class.getClassLoader(), new Class<?>[] {WorkerMetrics.class}, (p, m, a) -> {
                    if (m.getName().equals("state")) {
                        published.add((WorkerMetrics.State) a[0]);
                    }
                    return null;
                });
        state = new WorkerState(clock, Duration.ofSeconds(60), metrics);
        CycleCommitter<String, String> committer = new CycleCommitter<>(sink,
                r -> ("reply|" + r.correlationId()).getBytes(StandardCharsets.UTF_8),
                e -> ("error|" + e.correlationId()).getBytes(StandardCharsets.UTF_8));
        retry = new CommitRetry<>(committer, probe, state, clock, PROBE_EVERY, ATTEMPTS, alerts::add);
    }

    private List<HandlerResult<String, String>> cycle() {
        handlerCalls.incrementAndGet(); // the Handler ran once for this Cycle
        IncomingRequest q = new IncomingRequest("high", 0, 7, "key-c1", "c1", java.util.Map.of(),
                "p".getBytes(StandardCharsets.UTF_8));
        return List.of(new HandlerResult<>(q, new Reply<>("c1", "key-c1", "r1"), null));
    }

    private static ReplyDestinationFault.Unavailable unavailable() {
        return new ReplyDestinationFault.Unavailable("down", null);
    }

    private static ReplyDestinationFault.PermissionDenied denied() {
        return new ReplyDestinationFault.PermissionDenied("denied", null);
    }

    private void assertSameResults() {
        for (var call : sink.calls) {
            assertEquals(1, call.size());
            assertEquals("c1", call.get(0).correlationId());
            assertEquals(7L, call.get(0).position());
        }
    }

    @Test
    void transientCommitFailureIsRetriedWithTheSameResultsAndCommitsOnce() {
        sink.failures.add(new IllegalStateException("blip"));

        Optional<CommitResult> out = retry.commit(cycle());

        assertTrue(out.isPresent());
        assertEquals(2, sink.calls.size(), "one failure then one success");
        assertSameResults();
        assertEquals(1, handlerCalls.get(), "no Handler re-run");
        assertFalse(retry.holding());
        assertNull(state.pauseReason());
        assertTrue(alerts.isEmpty());
    }

    @Test
    void successOnTheLastAllowedAttemptDoesNotPause() {
        sink.failures.add(new IllegalStateException("1"));
        sink.failures.add(new IllegalStateException("2"));

        assertTrue(retry.commit(cycle()).isPresent());

        assertEquals(ATTEMPTS, sink.calls.size());
        assertNull(state.pauseReason());
    }

    @Test
    void exhaustedRetriesPauseDestinationAlertAndHoldResults() {
        sink.alwaysFail = true;
        sink.alwaysFailWith = new IllegalStateException("commit broken");

        Optional<CommitResult> out = retry.commit(cycle());

        assertTrue(out.isEmpty(), "results held, not committed");
        assertEquals(ATTEMPTS, sink.calls.size(), "exactly commit-retry-attempts attempts");
        assertSameResults();
        assertEquals(PauseReason.DESTINATION, state.pauseReason());
        assertEquals(Status.PAUSED, state.evaluate());
        assertEquals(WorkerMetrics.State.PAUSED, published.get(published.size() - 1));
        assertTrue(retry.holding());
        assertEquals(1, alerts.size());
        assertEquals(PauseReason.DESTINATION, alerts.get(0).reason());
        assertEquals(1, handlerCalls.get());
    }

    @Test
    void destinationUnavailablePausesAtOnceWithoutBurningRetries() {
        sink.alwaysFail = true;
        sink.alwaysFailWith = unavailable();

        assertTrue(retry.commit(cycle()).isEmpty());

        assertEquals(PauseReason.DESTINATION, state.pauseReason());
        assertEquals("request_reply.reply_destination.unavailable", alerts.get(0).faultId());
        assertTrue(retry.holding());
    }

    @Test
    void permissionDeniedPausesPermissionAndAlertsConfigurationFault() {
        sink.alwaysFail = true;
        sink.alwaysFailWith = denied();

        assertTrue(retry.commit(cycle()).isEmpty());

        assertEquals(1, sink.calls.size(), "a denial is not retried");
        assertEquals(PauseReason.PERMISSION, state.pauseReason());
        assertEquals(1, alerts.size());
        assertEquals(PauseReason.PERMISSION, alerts.get(0).reason());
        assertEquals("request_reply.reply_destination.permission_denied", alerts.get(0).faultId());
        assertTrue(retry.holding());
    }

    @Test
    void probesOnlyAfterTheIntervalAndKeepsHoldingWhileTheDestinationIsDown() {
        sink.alwaysFail = true;
        sink.alwaysFailWith = unavailable();
        retry.commit(cycle());
        probe.failWith = unavailable();

        clock.advance(PROBE_EVERY.minusSeconds(1));
        assertTrue(retry.tick().isEmpty());
        assertEquals(0, probe.calls, "too early to probe");

        clock.advance(Duration.ofSeconds(1));
        assertTrue(retry.tick().isEmpty());
        assertEquals(1, probe.calls);

        assertTrue(retry.tick().isEmpty());
        assertEquals(1, probe.calls, "next probe waits a full interval");
        clock.advance(PROBE_EVERY);
        retry.tick();
        assertEquals(2, probe.calls);

        assertTrue(retry.holding());
        assertEquals(PauseReason.DESTINATION, state.pauseReason());
        assertEquals(1, sink.calls.size(), "no commit while the probe fails");
        assertEquals(1, alerts.size(), "alerted once, not on every probe");
    }

    @Test
    void destinationReturnsRecommitsHeldResultsInANewTransactionAndResumes() {
        sink.alwaysFail = true;
        sink.alwaysFailWith = unavailable();
        retry.commit(cycle());
        int callsBefore = sink.calls.size();
        sink.alwaysFail = false;

        clock.advance(PROBE_EVERY);
        Optional<CommitResult> out = retry.tick();

        assertTrue(out.isPresent());
        assertEquals(callsBefore + 1, sink.calls.size(), "one new commit transaction");
        assertSameResults();
        assertEquals(1, handlerCalls.get(), "Handlers not re-run");
        assertFalse(retry.holding());
        assertNull(state.pauseReason());
        assertEquals(WorkerMetrics.State.RUNNING, published.get(published.size() - 1));
        assertTrue(retry.tick().isEmpty(), "nothing held, nothing to do");
        assertEquals(1, alerts.size());
    }

    @Test
    void permissionRestoredRecommitsTheCycleInProgressAndResumes() {
        sink.alwaysFail = true;
        sink.alwaysFailWith = denied();
        retry.commit(cycle());
        sink.alwaysFail = false;

        clock.advance(PROBE_EVERY);
        Optional<CommitResult> out = retry.tick();

        assertTrue(out.isPresent());
        assertSameResults();
        assertEquals(1, handlerCalls.get());
        assertNull(state.pauseReason());
        assertFalse(retry.holding());
    }

    @Test
    void probeOkButRecommitFailsAgainStaysPausedAndKeepsHolding() {
        sink.alwaysFail = true;
        sink.alwaysFailWith = unavailable();
        retry.commit(cycle());

        clock.advance(PROBE_EVERY);
        assertTrue(retry.tick().isEmpty(), "probe ok, commit still fails");

        assertTrue(retry.holding());
        assertEquals(PauseReason.DESTINATION, state.pauseReason());
        assertEquals(1, handlerCalls.get());
    }

    @Test
    void faultKindChangingWhileHeldMovesThePauseReason() {
        sink.alwaysFail = true;
        sink.alwaysFailWith = unavailable();
        retry.commit(cycle());
        probe.failWith = denied();

        clock.advance(PROBE_EVERY);
        retry.tick();

        assertEquals(PauseReason.PERMISSION, state.pauseReason());
        assertTrue(retry.holding());
    }
}
