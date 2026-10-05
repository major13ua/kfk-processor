package xme.common.kfkprocessor.requestreply.performance;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;
import static xme.common.kfkprocessor.requestreply.performance.PerfHarness.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** AC-10 and QG-1: several workers drain a backlog under one shared Rate Budget, one worker restarted on the way. */
@Testcontainers
class RateBudgetAccuracyIT {

    /**
     * CI scale: 15 s of catch-up at 200/s. Test-plan scale (5 minutes of backlog at 2,000/s, 10 minutes of catch-up)
     * runs pre-release: {@code ./gradlew preReleaseTest -Dperf.rate.budget=2000 -Dperf.rate.backlog=600000}.
     */
    private static final long BUDGET = Long.getLong("perf.rate.budget", 200);
    private static final int N = Integer.getInteger("perf.rate.backlog", 3000);
    private static final int WORKERS = 3;

    @Test
    void acceptedRequestsAcrossWorkersStayWithinBudgetInAnySlidingSecondDuringCatchUp() {
        catchUp(false);
    }

    @Test
    void acceptedRequestsStayWithinBudgetInAnySlidingSecondAlsoWhenOneWorkerIsRestartedDuringCatchUp() {
        catchUp(true);
    }

    private void catchUp(boolean restartOne) {
        Group g = Group.fresh();
        String topic = "req-" + g.suffix();
        createTopic(topic, 6);
        List<LaneSpec> lanes = List.of(new LaneSpec("main", topic, 1));
        produce(topic, "rb", N, i -> "k" + i);

        Starts starts = new Starts();
        Set<String> distinct = ConcurrentHashMap.newKeySet();
        List<Worker> workers = Collections.synchronizedList(new ArrayList<>());
        double[] retired = {0};
        List<Long> intakeTimes = Collections.synchronizedList(new ArrayList<>());
        Thread sampler = new Thread(() -> {
            double seen = 0;
            while (!Thread.currentThread().isInterrupted()) {
                double now;
                synchronized (workers) {
                    now = retired[0] + sum(workers, "requestreply.accepted", "lane", "main");
                }
                long t = System.currentTimeMillis();
                for (; seen < now; seen++) {
                    intakeTimes.add(t);
                }
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "intake-sampler");
        sampler.setDaemon(true);
        sampler.start();
        try {
            for (int i = 0; i < WORKERS; i++) {
                workers.add(startWorker(props(g, g.identity(i), BUDGET, 100, lanes), (ctx, req) -> {
                    starts.mark();
                    distinct.add(ctx.correlationId());
                    return "pong:" + req;
                }));
            }
            if (restartOne) {
                await("a third of the backlog accepted", WAIT, () -> starts.count() >= N / 3);
                Worker old;
                synchronized (workers) {
                    old = workers.remove(1);
                    retired[0] += sum(List.of(old), "requestreply.accepted", "lane", "main");
                }
                old.close();
                workers.add(startWorker(props(g, g.identity(1), BUDGET, 100, lanes), (ctx, req) -> {
                    starts.mark();
                    distinct.add(ctx.correlationId());
                    return "pong:" + req;
                }));
            }
            await("backlog drained", Duration.ofSeconds(120 + 2 * N / BUDGET), () -> distinct.size() >= N);
        } finally {
            sampler.interrupt();
            workers.forEach(Worker::close);
        }

        List<Long> intake;
        synchronized (intakeTimes) {
            intake = new ArrayList<>(intakeTimes);
        }
        System.out.println("[T17 AC-10] restart=" + restartOne + " diagnostic: max in any sliding 1 s measured at intake (allowance taken)="
                + maxInAnySecond(intake) + " of " + intake.size() + " units");
        List<Long> times = starts.sorted();
        int max = maxInAnySecond(times);
        int allowed = (int) Math.floor(BUDGET * BUDGET_TOLERANCE);
        System.out.println("[T17 AC-10] restart=" + restartOne + " budget=" + BUDGET + "/s workers=" + WORKERS + " handled=" + times.size()
                + " max in any sliding 1 s=" + max + " allowed=" + allowed + " perSecond=" + perSecond(times));
        assertThat(max).as("most requests handed to a Handler in any sliding 1 s window (budget " + BUDGET
                + " x " + BUDGET_TOLERANCE + "); accepted per second: " + perSecond(times))
                .isLessThanOrEqualTo(allowed);
    }
}
