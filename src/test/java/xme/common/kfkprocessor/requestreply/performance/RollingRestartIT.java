package xme.common.kfkprocessor.requestreply.performance;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;
import static xme.common.kfkprocessor.requestreply.performance.PerfHarness.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** AC-14 and QG-3: workers restarted one by one with the same identity inside the identity window. */
@Testcontainers(disabledWithoutDocker = true)
class RollingRestartIT {

    private static final String MEMBERSHIP = "requestreply.group.membership.changes";
    private static final int WORKERS = 3;
    private static final Duration IDENTITY_WINDOW = Duration.ofSeconds(10); // FailureHarness.workerProps value

    @Test
    void rollingRestartWithReturningIdentitiesCausesNoMembershipChangeForTheOtherWorkersAndTheyKeepConsuming() {
        Group g = Group.fresh();
        String topic = "req-" + g.suffix();
        createTopic(topic, WORKERS);
        List<LaneSpec> lanes = List.of(new LaneSpec("main", topic, 1));
        produce(topic, "rr", 8000, i -> "k" + i);

        AtomicInteger[] handled = new AtomicInteger[WORKERS];
        Worker[] workers = new Worker[WORKERS];
        try {
            for (int i = 0; i < WORKERS; i++) {
                workers[i] = start(g, i, lanes, handled);
            }
            for (int i = 0; i < WORKERS; i++) {
                int w = i;
                await("worker " + w + " consuming its lanes", WAIT, () -> handled[w].get() >= 20);
            }
            pause(Duration.ofSeconds(3)); // initial joins settle

            for (int restarting = 0; restarting < WORKERS; restarting++) {
                List<Integer> others = new ArrayList<>();
                List<Double> changesBefore = new ArrayList<>();
                List<Integer> handledBefore = new ArrayList<>();
                for (int i = 0; i < WORKERS; i++) {
                    if (i != restarting) {
                        others.add(i);
                        changesBefore.add(count(workers[i], MEMBERSHIP));
                        handledBefore.add(handled[i].get());
                    }
                }
                long closedAt = System.currentTimeMillis();
                workers[restarting].close();
                handled[restarting] = new AtomicInteger();
                workers[restarting] = start(g, restarting, lanes, handled);
                long backAt = System.currentTimeMillis();
                int r = restarting;
                await("returning worker " + r + " consuming again", WAIT, () -> handled[r].get() >= 5);

                assertThat(backAt - closedAt).as("returned within the identity window (precondition)")
                        .isLessThan(IDENTITY_WINDOW.toMillis());
                for (int k = 0; k < others.size(); k++) {
                    int o = others.get(k);
                    assertThat(count(workers[o], MEMBERSHIP))
                            .as("group membership change counter of worker " + o + " during the restart of worker "
                                    + restarting + " (back after " + (backAt - closedAt) + " ms)")
                            .isEqualTo(changesBefore.get(k));
                    assertThat(handled[o].get()).as("worker " + o + " kept consuming during the restart of " + restarting)
                            .isGreaterThan(handledBefore.get(k));
                }
            }
        } finally {
            for (Worker w : workers) {
                if (w != null) {
                    w.close();
                }
            }
        }
    }

    private static Worker start(Group g, int i, List<LaneSpec> lanes, AtomicInteger[] handled) {
        if (handled[i] == null) {
            handled[i] = new AtomicInteger();
        }
        AtomicInteger mine = handled[i];
        return startWorker(props(g, g.identity(i), 100, 50, lanes), (ctx, req) -> {
            mine.incrementAndGet();
            return "pong:" + req;
        });
    }
}
