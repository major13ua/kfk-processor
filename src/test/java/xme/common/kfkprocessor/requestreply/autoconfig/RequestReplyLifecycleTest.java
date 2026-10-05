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

        lifecycle.start();
        Thread.sleep(500);
        lifecycle.stop();

        assertEquals(0, lanes.fetches.get(), "the gate does not fetch (no records can be buffered while denied)");
        assertTrue(lanes.keepAlives.get() >= 1, "membership kept alive, was " + lanes.keepAlives.get());
        assertTrue(lanes.keepAlives.get() <= 50, "no busy spin: " + lanes.keepAlives.get() + " keep-alive rounds in 0.5 s");
        assertTrue(probes.get() >= 2, "probed again on the probe interval, was " + probes.get());
    }
}
