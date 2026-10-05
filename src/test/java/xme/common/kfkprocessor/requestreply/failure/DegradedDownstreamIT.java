package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** AC-19: Handlers hit their timeout against a slow downstream; Requesters get Error Replies at a bounded rate. */
@Testcontainers
class DegradedDownstreamIT {

    private static final long BUDGET = 10;
    private static final int N = 30;

    @Test
    void handlersTimingOutYieldTimeoutErrorRepliesWithoutRetriesAndWithinBudget() {
        Ids ids = Ids.fresh();
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("dd", N);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(workerProps(ids, BUDGET, (int) BUDGET, Duration.ofSeconds(1), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    long end = System.nanoTime() + Duration.ofSeconds(4).toNanos(); // degraded: slower than the timeout
                    while (System.nanoTime() < end && !ctx.cancellation().isCancelled()) {
                        Thread.sleep(10);
                    }
                    return "late:" + req;
                })) {
            List<Rep> replies = awaitReplies(ids.replies(), N, WAIT, Duration.ofSeconds(3));

            assertReconciled(requested, replies);
            assertThat(replies).as("every Requester got an Error Reply naming the timeout")
                    .allSatisfy(r -> {
                        assertThat(r.body()).contains("\"category\":\"timeout\"");
                        assertThat(r.body()).contains("\"correlation_id\":\"" + r.correlationId() + "\"");
                        assertThat(r.requestKey()).isEqualTo(r.correlationId());
                    });
            assertThat(w.errorReplies("TIMEOUT")).as("timeout error replies counted").isEqualTo((double) N);
            assertThat(handled.count()).as("no retry storm: one Handler invocation per request").isEqualTo(N);
            assertThat(handled.perCorrelation().values()).containsOnly(1);
            assertThat(maxInAnySecond(handled.times()))
                    .as("most requests accepted in any sliding 1 s window (budget " + BUDGET + ")")
                    .isLessThanOrEqualTo((int) Math.floor(BUDGET * BUDGET_TOLERANCE));
        }
    }
}
