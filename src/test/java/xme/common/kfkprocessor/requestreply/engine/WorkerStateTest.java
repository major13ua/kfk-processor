package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.PauseReason;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.Status;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

class WorkerStateTest {

    private static final Duration THRESHOLD = Duration.ofSeconds(60);

    private static final class FakeClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        @Override public Instant instant() { return now; }
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId z) { return this; }
    }

    private FakeClock clock;
    private List<WorkerMetrics.State> published;
    private WorkerState state;

    @BeforeEach
    void setUp() {
        clock = new FakeClock();
        published = new ArrayList<>();
        WorkerMetrics metrics = (WorkerMetrics) Proxy.newProxyInstance(
                WorkerMetrics.class.getClassLoader(), new Class<?>[] {WorkerMetrics.class}, (p, m, a) -> {
                    if (m.getName().equals("state")) {
                        published.add((WorkerMetrics.State) a[0]);
                    }
                    return null;
                });
        state = new WorkerState(clock, THRESHOLD, metrics);
    }

    // AC-17
    @Test
    void nothingPendingForHours_isIdle_noStall() {
        clock.advance(Duration.ofHours(5));
        assertEquals(Status.IDLE, state.evaluate());
        assertEquals(List.of(), published.stream().filter(s -> s == WorkerMetrics.State.STALLED).toList());
    }

    // AC-17 (A5): the stall clock starts when work arrives, not at the last commit before a long idle
    @Test
    void firstWorkAfterLongIdle_isRunningNotStalled() {
        clock.advance(Duration.ofMinutes(10));
        state.setPendingWork(true);
        state.setCycleOpen(true);
        assertEquals(Status.RUNNING, state.evaluate());
        assertEquals(false, published.contains(WorkerMetrics.State.STALLED));
        clock.advance(Duration.ofSeconds(61));
        assertEquals(Status.STALLED, state.evaluate());
    }

    @Test
    void pendingWorkWithinThreshold_isRunning() {
        state.setPendingWork(true);
        clock.advance(Duration.ofSeconds(59));
        assertEquals(Status.RUNNING, state.evaluate());
    }

    @Test
    void pendingWorkBeyondThresholdWithoutCommit_isStalled() {
        state.setPendingWork(true);
        clock.advance(Duration.ofSeconds(61));
        assertEquals(Status.STALLED, state.evaluate());
    }

    @Test
    void openCycleBeyondThresholdWithoutCommit_isStalled() {
        state.setCycleOpen(true);
        clock.advance(Duration.ofSeconds(61));
        assertEquals(Status.STALLED, state.evaluate());
    }

    @Test
    void stallMeasuredFromLastCommit() {
        state.setPendingWork(true);
        clock.advance(Duration.ofSeconds(50));
        state.commitSucceeded();
        clock.advance(Duration.ofSeconds(50));
        assertEquals(Status.RUNNING, state.evaluate());
        clock.advance(Duration.ofSeconds(11));
        assertEquals(Status.STALLED, state.evaluate());
    }

    @Test
    void commitClearsStall() {
        state.setPendingWork(true);
        clock.advance(Duration.ofSeconds(120));
        assertEquals(Status.STALLED, state.evaluate());
        state.commitSucceeded();
        assertEquals(Status.RUNNING, state.evaluate());
    }

    @Test
    void workDrainedAfterStall_becomesIdle() {
        state.setPendingWork(true);
        clock.advance(Duration.ofSeconds(120));
        assertEquals(Status.STALLED, state.evaluate());
        state.setPendingWork(false);
        assertEquals(Status.IDLE, state.evaluate());
    }

    @Test
    void pausedForEachReason_isPaused_andNeverStalledBeyondThreshold() {
        for (PauseReason reason : PauseReason.values()) {
            setUp();
            state.setPendingWork(true);
            state.setCycleOpen(true);
            state.pause(reason);
            clock.advance(Duration.ofMinutes(10));
            assertEquals(Status.PAUSED, state.evaluate(), reason.name());
            assertEquals(reason, state.pauseReason());
            assertEquals(false, published.contains(WorkerMetrics.State.STALLED), reason.name());
        }
    }

    @Test
    void pauseWithNothingPending_isPausedNotIdle() {
        state.pause(PauseReason.PERMISSION);
        assertEquals(Status.PAUSED, state.evaluate());
    }

    @Test
    void pausePrecedesStall_evenWhenAlreadyStalled() {
        state.setPendingWork(true);
        clock.advance(Duration.ofSeconds(90));
        assertEquals(Status.STALLED, state.evaluate());
        state.pause(PauseReason.DESTINATION);
        assertEquals(Status.PAUSED, state.evaluate());
    }

    @Test
    void resumeAfterLongPause_doesNotInstantlyStall() {
        state.setPendingWork(true);
        state.pause(PauseReason.LIMITER);
        clock.advance(Duration.ofMinutes(10));
        state.resume(PauseReason.LIMITER);
        assertNull(state.pauseReason());
        assertEquals(Status.RUNNING, state.evaluate());
        clock.advance(Duration.ofSeconds(61));
        assertEquals(Status.STALLED, state.evaluate());
    }

    @Test
    void publishesOncePerChange_mappingIdleAndRunningToRunning() {
        state.evaluate();
        state.evaluate();
        state.setPendingWork(true);
        state.evaluate();
        state.evaluate();
        clock.advance(Duration.ofSeconds(61));
        state.evaluate();
        state.evaluate();
        state.pause(PauseReason.LIMITER);
        state.evaluate();
        state.evaluate();
        state.resume(PauseReason.LIMITER);
        state.commitSucceeded();
        state.evaluate();
        state.evaluate();
        for (int i = 1; i < published.size(); i++) {
            assertEquals(false, published.get(i) == published.get(i - 1), "duplicate publish at " + i);
        }
        List<WorkerMetrics.State> tail = published.subList(Math.max(0, published.size() - 3), published.size());
        assertEquals(
                List.of(WorkerMetrics.State.STALLED, WorkerMetrics.State.PAUSED, WorkerMetrics.State.RUNNING), tail);
    }
}
