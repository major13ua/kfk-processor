package xme.common.kfkprocessor.requestreply.adapters.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/** Micrometer implementation of {@link WorkerMetrics}. Tags: only lane and category. */
public class MicrometerWorkerMetrics implements WorkerMetrics {

    private final MeterRegistry registry;
    private final Map<String, Counter> accepted = new ConcurrentHashMap<>();
    private final Map<String, Counter> implausible = new ConcurrentHashMap<>();
    private final Map<String, Timer> lag = new ConcurrentHashMap<>();
    private final Map<ErrorCategory, Counter> errorReplies = new ConcurrentHashMap<>();
    private final AtomicInteger stateValue = new AtomicInteger(State.RUNNING.ordinal());
    private final Counter handlerTimeout;
    private final Counter commitAttempts;
    private final Counter membershipChanges;
    private final Timer cycleDuration;

    public MicrometerWorkerMetrics(MeterRegistry registry) {
        this.registry = registry;
        // Strong reference held by this field so the gauge is not garbage collected.
        registry.gauge("requestreply.state", stateValue);
        this.handlerTimeout = registry.counter("requestreply.handler.timeout");
        this.commitAttempts = registry.counter("requestreply.commit.attempts");
        this.membershipChanges = registry.counter("requestreply.group.membership.changes");
        this.cycleDuration = registry.timer("requestreply.cycle.duration");
    }

    @Override
    public void accepted(String lane, long count) {
        accepted.computeIfAbsent(lane, l -> registry.counter("requestreply.accepted", "lane", l)).increment(count);
    }

    @Override
    public void consistencyLag(String lane, Duration lagValue, boolean isImplausible) {
        if (isImplausible || lagValue.isNegative()) {
            implausible.computeIfAbsent(lane,
                    l -> registry.counter("requestreply.consistency.lag.implausible", "lane", l)).increment();
            return;
        }
        lag.computeIfAbsent(lane, l -> registry.timer("requestreply.consistency.lag", "lane", l)).record(lagValue);
    }

    @Override
    public void state(State state) {
        stateValue.set(state.ordinal());
    }

    @Override
    public void errorReply(ErrorCategory category) {
        errorReplies.computeIfAbsent(category,
                c -> registry.counter("requestreply.errorreply", "category", c.name())).increment();
    }

    @Override public void handlerTimeout() { handlerTimeout.increment(); }
    @Override public void commitAttempt() { commitAttempts.increment(); }
    @Override public void cycleDuration(Duration duration) { cycleDuration.record(duration); }
    @Override public void groupMembershipChange() { membershipChanges.increment(); }
}
