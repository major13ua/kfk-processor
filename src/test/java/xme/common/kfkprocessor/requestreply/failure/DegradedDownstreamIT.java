package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * AC-19: the downstream degrades. First Handlers fail fast (the retry-storm risk: a failing Handler frees the worker
 * at once, so only the Rate Budget keeps the intake bounded), then they hit their timeout. Requesters get Error
 * Replies, and the accepted-request metric never exceeds the budget in any 1 s window.
 */
@Testcontainers
class DegradedDownstreamIT {

    /** Spec section 1: budgets below 20 have no margin for the accuracy assertion; 50 gives a burst capacity of 2. */
    private static final long BUDGET = 50;
    private static final int N = 500;
    /** Requests from this index on hit the Handler timeout; those before fail fast. */
    private static final int FIRST_SLOW = N - 10;

    private record Sample(long atMillis, double accepted) {
    }

    private static double accepted(Worker w) {
        return w.registry.find("requestreply.accepted").counters().stream().mapToDouble(c -> c.count()).sum();
    }

    @Test
    void failingAndTimingOutHandlersYieldErrorRepliesWithoutRetriesAndAcceptedStaysWithinBudget() throws Exception {
        Ids ids = Ids.fresh();
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("dd", N);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(workerProps(ids, BUDGET, (int) BUDGET, Duration.ofSeconds(1), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    int index = Integer.parseInt(ctx.correlationId().substring(ctx.correlationId().lastIndexOf('-') + 1));
                    if (index < FIRST_SLOW) {
                        throw new IllegalStateException("downstream refused"); // fails fast
                    }
                    long end = System.nanoTime() + Duration.ofSeconds(4).toNanos(); // degraded: slower than the timeout
                    while (System.nanoTime() < end && !ctx.cancellation().isCancelled()) {
                        Thread.sleep(10);
                    }
                    return "late:" + req;
                })) {
            List<Sample> samples = new CopyOnWriteArrayList<>();
            Thread sampler = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    samples.add(new Sample(System.currentTimeMillis(), accepted(w)));
                    try {
                        Thread.sleep(25);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            sampler.setDaemon(true);
            sampler.start();
            List<Rep> replies;
            try {
                replies = awaitReplies(ids.replies(), N, WAIT.multipliedBy(2), Duration.ofSeconds(3));
            } finally {
                sampler.interrupt();
                sampler.join();
            }

            assertReconciled(requested, replies);
            assertThat(replies).as("every Requester got an Error Reply").allSatisfy(r -> {
                assertThat(r.type()).isEqualTo("error_reply");
                assertThat(r.body()).contains("\"correlation_id\":\"" + r.correlationId() + "\"");
                assertThat(r.requestKey()).isEqualTo(r.correlationId());
            });
            List<Rep> failing = replies.stream().filter(r -> index(r) < FIRST_SLOW).toList();
            List<Rep> slow = replies.stream().filter(r -> index(r) >= FIRST_SLOW).toList();
            assertThat(failing).hasSize(FIRST_SLOW)
                    .allSatisfy(r -> assertThat(r.body()).contains("\"category\":\"failure\""));
            assertThat(slow).hasSize(N - FIRST_SLOW)
                    .allSatisfy(r -> assertThat(r.body()).contains("\"category\":\"timeout\""));
            assertThat(w.errorReplies("FAILURE")).isEqualTo((double) FIRST_SLOW);
            assertThat(w.errorReplies("TIMEOUT")).isEqualTo((double) (N - FIRST_SLOW));
            assertThat(handled.count()).as("no retry storm: one Handler invocation per request").isEqualTo(N);
            assertThat(handled.perCorrelation().values()).containsOnly(1);

            double total = samples.get(samples.size() - 1).accepted();
            assertThat(total).as("requestreply.accepted counts every request taken in").isGreaterThanOrEqualTo(N);
            double worst = maxAcceptedInAnyWindow(samples, Duration.ofSeconds(1));
            assertThat(worst).as("requestreply.accepted in any 1 s window (budget " + BUDGET + ")")
                    .isLessThanOrEqualTo(BUDGET * BUDGET_TOLERANCE);
            // not by construction: the budget, not the Handlers, was what held the intake back during the fast phase
            assertThat(worst).as("the budget was the binding limit (accepted reached it)")
                    .isGreaterThanOrEqualTo(BUDGET * 0.5);
        }
    }

    private static int index(Rep r) {
        return Integer.parseInt(r.correlationId().substring(r.correlationId().lastIndexOf('-') + 1));
    }

    /** Largest increase of the cumulative counter over any window of at least {@code window}. */
    private static double maxAcceptedInAnyWindow(List<Sample> samples, Duration window) {
        double max = 0;
        int lo = 0;
        for (int hi = 0; hi < samples.size(); hi++) {
            // newest start sample that is at least one window older than the end sample
            while (lo + 1 < hi && samples.get(lo + 1).atMillis() <= samples.get(hi).atMillis() - window.toMillis()) {
                lo++;
            }
            if (samples.get(hi).atMillis() - samples.get(lo).atMillis() >= window.toMillis()) {
                max = Math.max(max, samples.get(hi).accepted() - samples.get(lo).accepted());
            }
        }
        return max;
    }
}
