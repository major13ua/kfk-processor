package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** AC-18 and QG-1: the Rate Budget store (Redis) is stopped and started again (same address, state lost). */
@Testcontainers
class RateBudgetStoreOutageIT {

    private static final long BUDGET = 20;
    private static final int N = 200;

    /** What one outage run observed. */
    private record Run(HandlerLog handled, List<String> requested, List<Rep> replies, long cutAt, long backAt,
            long firstRequestAt) {
    }

    @Test
    void storeStoppedStopsNewRequestsWithinTargetAndResumesWithinTargetWithNothingLostOrRerun() {
        Run run = outage(true);
        assertReconciled(run.requested(), run.replies());
        assertThat(run.handled().perCorrelation().values()).as("no Handler re-run").containsOnly(1);
    }

    @Test
    void acceptedRateStaysWithinBudgetInAnySlidingSecondAcrossTheOutage() {
        Run run = outage(false);
        int max = maxInAnySecond(run.handled().times());
        long t0 = run.firstRequestAt();
        Map<Long, Long> perSecond = run.handled().times().stream().collect(
                Collectors.groupingBy(t -> (t - t0) / 1000, TreeMap::new, Collectors.counting()));
        assertThat(max).as("most requests accepted in any sliding 1 s window (budget " + BUDGET + "); store cut at +"
                + (run.cutAt() - t0) + " ms, back at +" + (run.backAt() - t0)
                + " ms; accepted per second since first request: " + perSecond)
                .isLessThanOrEqualTo((int) Math.floor(BUDGET * BUDGET_TOLERANCE));
    }

    /** Runs the backlog through a worker, stops Redis mid-run, starts it again; asserts stop/resume timing if asked. */
    private Run outage(boolean assertTiming) {
        Ids ids = Ids.fresh();
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("rb", N);
        produce(ids.requests(), requested);
        var docker = REDIS.getDockerClient();

        try (Worker w = startWorker(workerProps(ids, BUDGET, (int) BUDGET, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                })) {
            await("intake flowing before the cut", WAIT, () -> handled.count() >= 40);

            long cutAt = System.currentTimeMillis();
            docker.stopContainerCmd(REDIS.getContainerId()).withTimeout(1).exec();
            long backAt;
            try {
                if (assertTiming) {
                    await("worker shows paused state within the stop target", STOP_WITHIN, () -> w.state() == 1.0);
                    // settle past the stop target (allowance reserved before the cut may still be served)
                    pause(Duration.ofMillis(cutAt + STOP_WITHIN.toMillis() + 3000 - System.currentTimeMillis()));
                    long stopDeadline = cutAt + STOP_WITHIN.toMillis();
                    long late = handled.times().stream().filter(t -> t > stopDeadline).count();
                    assertThat(late).as("Handler starts later than " + STOP_WITHIN + " after the store was cut").isZero();
                    assertThat(w.state()).as("paused while the store is down").isEqualTo(1.0);
                    assertThat(inGroup(ids.group(), ids.identity())).as("stays in its group").isTrue();
                } else {
                    pause(Duration.ofSeconds(8));
                }
            } finally {
                docker.startContainerCmd(REDIS.getContainerId()).exec();
                backAt = System.currentTimeMillis();
            }

            int before = handled.count();
            await("intake resumes within the resume target", RESUME_WITHIN, () -> handled.count() > before);
            if (assertTiming) {
                long resumedAfter = handled.times().stream().filter(t -> t > backAt).findFirst().orElse(-1L) - backAt;
                assertThat(resumedAfter).as("ms from store return to first new request")
                        .isBetween(0L, RESUME_WITHIN.toMillis());
            }
            List<Rep> replies = awaitReplies(ids.replies(), N, WAIT, Duration.ofSeconds(2));
            if (assertTiming) {
                assertThat(w.state()).as("running again").isEqualTo(0.0);
            }
            return new Run(handled, requested, replies, cutAt, backAt, handled.times().get(0));
        }
    }
}
