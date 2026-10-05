package xme.common.kfkprocessor.requestreply.autoconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.engine.CycleLoop;
import xme.common.kfkprocessor.requestreply.engine.WorkerState;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/** Startup gate (AC-09): while the reply destination is denied the worker keeps its membership without spinning. */
class RequestReplyLifecycleTest {

    private static final class CountingLanes implements RequestLanes {
        final AtomicInteger fetches = new AtomicInteger();
        final AtomicInteger keepAlives = new AtomicInteger();

        @Override
        public List<IncomingRequest> fetch(Map<String, Integer> quotaByLane) {
            fetches.incrementAndGet();
            return List.of();
        }

        @Override
        public void keepAlive() {
            keepAlives.incrementAndGet();
        }

        @Override
        public void pause() {
        }

        @Override
        public void resume() {
        }
    }

    // review r2 D: the permission gate keeps the member alive with keepAlive() and sleeps between rounds
    @Test
    void permissionGateKeepsTheMemberAliveWithoutBusySpinning() throws Exception {
        CountingLanes lanes = new CountingLanes();
        AtomicInteger probes = new AtomicInteger();
        WorkerState state = new WorkerState(Clock.systemUTC(), Duration.ofSeconds(60), mock(WorkerMetrics.class));
        CycleLoop<?, ?, ?> loop = new CycleLoop<>(null, null, null, state, null, Clock.systemUTC(),
                Duration.ofSeconds(5), r -> null, Duration.ofHours(1), Duration.ofMillis(200), a -> { });
        var lifecycle = new RequestReplyLifecycle(loop, lanes, () -> {
            probes.incrementAndGet();
            throw new ReplyDestinationFault.PermissionDenied("no WRITE", null);
        }, state, a -> { }, Duration.ofMillis(200), true);

        long started = System.nanoTime();
        lifecycle.start();
        waitFor("a keep-alive and a second probe", Duration.ofSeconds(10),
                () -> lanes.keepAlives.get() >= 1 && probes.get() >= 2);
        long elapsedMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
        lifecycle.stop();

        assertEquals(0, lanes.fetches.get(), "the gate does not fetch (no records can be buffered while denied)");
        assertTrue(lanes.keepAlives.get() >= 1, "membership kept alive, was " + lanes.keepAlives.get());
        assertTrue(lanes.keepAlives.get() <= 10 + elapsedMs / 10,
                "no busy spin: " + lanes.keepAlives.get() + " keep-alive rounds in " + elapsedMs + " ms");
        assertTrue(probes.get() >= 2, "probed again on the probe interval, was " + probes.get());
    }

    // F27 / AC-09: an Invalid destination refuses the start; the loop never runs and nothing is fetched
    @Test
    void invalidDestinationRefusesStartAndNeverStartsTheLoop() throws Exception {
        CountingLanes lanes = new CountingLanes();
        WorkerState state = new WorkerState(Clock.systemUTC(), Duration.ofSeconds(60), mock(WorkerMetrics.class));
        CycleLoop<?, ?, ?> loop = new CycleLoop<>(null, null, null, state, null, Clock.systemUTC(),
                Duration.ofSeconds(5), r -> null, Duration.ofHours(1), Duration.ofMillis(200), a -> { });
        var lifecycle = new RequestReplyLifecycle(loop, lanes, () -> {
            throw new ReplyDestinationFault.Invalid(
                    "invalid topic name 'bad topic' [request_reply.config.reply_destination_invalid]", null);
        }, state, a -> { }, Duration.ofMillis(200), true);

        java.util.Set<Thread> before = loopThreads();
        RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, lifecycle::start);
        assertTrue(e.getMessage().contains("request_reply.config."), e.getMessage());
        assertTrue(e.getMessage().contains("bad topic"), e.getMessage());
        assertTrue(!lifecycle.isRunning(), "not running after a refused start");
        // CycleLoop.start() spawns these two threads synchronously and (with the null intake) the loop thread keeps
        // living on its idle backoff, so a started loop is observable right here without waiting
        java.util.Set<Thread> started = loopThreads();
        started.removeAll(before);
        assertTrue(started.isEmpty(), "a refused start must not start the loop, new threads: " + started);
        assertEquals(0, lanes.fetches.get(), "loop never fetched");
        assertEquals(0, lanes.keepAlives.get(), "no permission gate keep-alive");
    }

    private static java.util.Set<Thread> loopThreads() {
        java.util.Set<Thread> found = new java.util.HashSet<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().equals("request-reply-cycle-loop") || t.getName().equals("request-reply-stall-watchdog")
                    || t.getName().equals("request-reply-permission-gate")) {
                found.add(t);
            }
        }
        return found;
    }

    private static void waitFor(String what, Duration timeout, java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out after " + timeout + " waiting for " + what);
            }
            Thread.sleep(10);
        }
    }
}
