package xme.common.kfkprocessor.requestreply.adapters.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics.State;

/** AC-16: Consistency Lag recorded per lane for committed replies; implausible samples flagged and excluded. */
class MicrometerWorkerMetricsTest {

    private SimpleMeterRegistry registry;
    private MicrometerWorkerMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerWorkerMetrics(registry);
    }

    @Test
    void validLagIsRecordedPerLane() {
        metrics.consistencyLag("a", Duration.ofMillis(200), false);
        metrics.consistencyLag("a", Duration.ofMillis(400), false);
        metrics.consistencyLag("b", Duration.ofMillis(100), false);

        var a = registry.get("requestreply.consistency.lag").tag("lane", "a").timer();
        var b = registry.get("requestreply.consistency.lag").tag("lane", "b").timer();
        assertEquals(2, a.count());
        assertEquals(600.0, a.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS), 0.001);
        assertEquals(1, b.count());
    }

    @Test
    void implausibleLagIsFlaggedAndExcludedFromTimer() {
        metrics.consistencyLag("a", Duration.ofMillis(200), false);
        metrics.consistencyLag("a", Duration.ofDays(3650), true);

        assertEquals(1, registry.get("requestreply.consistency.lag").tag("lane", "a").timer().count());
        assertEquals(1.0, registry.get("requestreply.consistency.lag.implausible").tag("lane", "a").counter().count());
    }

    @Test
    void negativeLagIsFlaggedAndExcludedEvenWhenCallerDidNotFlagIt() {
        metrics.consistencyLag("a", Duration.ofMillis(-5), false);

        var timer = registry.find("requestreply.consistency.lag").tag("lane", "a").timer();
        assertTrue(timer == null || timer.count() == 0, "negative sample must not be timed");
        assertEquals(1.0, registry.get("requestreply.consistency.lag.implausible").tag("lane", "a").counter().count());
    }

    @Test
    void validLagDoesNotIncrementFlagCounter() {
        metrics.consistencyLag("a", Duration.ofMillis(10), false);
        var c = registry.find("requestreply.consistency.lag.implausible").tag("lane", "a").counter();
        assertTrue(c == null || c.count() == 0);
    }

    @Test
    void acceptedCountsPerLane() {
        metrics.accepted("a", 3);
        metrics.accepted("a", 2);
        metrics.accepted("b", 1);
        assertEquals(5.0, registry.get("requestreply.accepted").tag("lane", "a").counter().count());
        assertEquals(1.0, registry.get("requestreply.accepted").tag("lane", "b").counter().count());
    }

    @Test
    void errorRepliesCountedByCategory() {
        metrics.errorReply(ErrorCategory.TIMEOUT);
        metrics.errorReply(ErrorCategory.TIMEOUT);
        metrics.errorReply(ErrorCategory.FAILURE);
        assertEquals(2.0, registry.get("requestreply.errorreply").tag("category", "TIMEOUT").counter().count());
        assertEquals(1.0, registry.get("requestreply.errorreply").tag("category", "FAILURE").counter().count());
    }

    @Test
    void simpleCountersAndTimers() {
        metrics.handlerTimeout();
        metrics.commitAttempt();
        metrics.commitAttempt();
        metrics.groupMembershipChange();
        metrics.cycleDuration(Duration.ofMillis(50));

        assertEquals(1.0, registry.get("requestreply.handler.timeout").counter().count());
        assertEquals(2.0, registry.get("requestreply.commit.attempts").counter().count());
        assertEquals(1.0, registry.get("requestreply.group.membership.changes").counter().count());
        var t = registry.get("requestreply.cycle.duration").timer();
        assertEquals(1, t.count());
        assertEquals(50.0, t.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS), 0.001);
    }

    /** Numeric gauge (tags are limited to lane and category): RUNNING=0, PAUSED=1, STALLED=2. */
    @Test
    void stateGaugeIsNumeric() {
        metrics.state(State.RUNNING);
        var g = registry.get("requestreply.state").gauge();
        assertEquals(0.0, g.value());
        metrics.state(State.PAUSED);
        assertEquals(1.0, g.value());
        metrics.state(State.STALLED);
        assertEquals(2.0, g.value());
        metrics.state(State.RUNNING);
        assertEquals(0.0, g.value());
    }

    @Test
    void onlyLaneAndCategoryTagKeysAnywhere() {
        metrics.accepted("a", 1);
        metrics.consistencyLag("a", Duration.ofMillis(1), false);
        metrics.consistencyLag("a", Duration.ofMillis(-1), false);
        metrics.state(State.STALLED);
        metrics.errorReply(ErrorCategory.UNDELIVERABLE);
        metrics.handlerTimeout();
        metrics.commitAttempt();
        metrics.cycleDuration(Duration.ofMillis(1));
        metrics.groupMembershipChange();

        assertTrue(registry.getMeters().size() >= 8);
        Set<String> allowed = Set.of("lane", "category");
        for (Meter m : registry.getMeters()) {
            assertTrue(m.getId().getName().startsWith("requestreply."), m.getId().getName());
            for (Tag t : m.getId().getTags()) {
                assertTrue(allowed.contains(t.getKey()), "unexpected tag " + t.getKey() + " on " + m.getId().getName());
            }
        }
        assertNotNull(registry.find("requestreply.state").gauge());
        assertNull(registry.find("requestreply.state").tag("lane", "a").gauge());
    }
}
