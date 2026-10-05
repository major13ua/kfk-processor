package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** AC-05 and QG-2: a failed Cycle commit is retried with the same results; Handlers are never re-run. */
@Testcontainers
class FailedCommitIT {

    private static final int N = 5;

    @Test
    void failedCommitThenSuccessKeepsHandlerCountAndGivesOneCommittedReplyPerRequest() {
        Ids ids = Ids.fresh();
        FaultInjection fault = new FaultInjection();
        fault.failNextCommits.set(1);
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("fc", N);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                }, fault.beans(ids.group()))) {
            List<Rep> replies = awaitReplies(ids.replies(), N, WAIT, Duration.ofSeconds(3));

            assertThat(fault.commitCalls.get()).as("commit attempts: one failed, then a success").isGreaterThanOrEqualTo(2);
            assertThat(handled.count()).as("Handler invocations").isEqualTo(N);
            assertThat(handled.perCorrelation().values()).as("each request run once").containsOnly(1);
            assertReconciled(requested, replies);
        }
    }

    @Test
    void commitFailingUntilRetriesAreExhaustedPausesThenRecommitsWithoutRerunningHandlers() {
        Ids ids = Ids.fresh();
        FaultInjection fault = new FaultInjection();
        fault.failNextCommits.set(1000); // far above the default commit-retry-attempts (3): the Cycle is held
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("fx", N);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                }, fault.beans(ids.group()))) {
            await("Handlers ran for the Cycle", WAIT, () -> handled.count() >= N);
            await("worker paused after the retries are exhausted", Duration.ofSeconds(15), () -> w.state() == 1.0);
            assertThat(fault.alerts).as("Operator alert raised for the held Cycle").isNotEmpty();
            pause(Duration.ofSeconds(1)); // several probe intervals: the held Cycle keeps failing and stays held
            assertThat(w.state()).as("still paused while the commit keeps failing").isEqualTo(1.0);
            assertThat(readCommitted(ids.replies(), Duration.ofSeconds(1))).as("nothing committed while held").isEmpty();
            assertThat(handled.count()).as("Handlers not re-run while held").isEqualTo(N);
            assertThat(inGroup(ids.group(), ids.identity())).as("stays in its group while paused").isTrue();

            fault.failNextCommits.set(0);

            List<Rep> replies = awaitReplies(ids.replies(), N, RESUME_WITHIN, Duration.ofSeconds(3));
            assertThat(fault.commitCalls.get()).as("3 failed attempts, probe-driven re-commits, then success")
                    .isGreaterThanOrEqualTo(4);
            assertThat(handled.count()).as("Handler invocations").isEqualTo(N);
            assertThat(handled.perCorrelation().values()).containsOnly(1);
            assertReconciled(requested, replies);
            await("worker running again", Duration.ofSeconds(5), () -> w.state() == 0.0);
        }
    }

    /**
     * Unknown commit outcome (a commit timeout): the commit really landed but the engine is told it failed. The
     * retry must resolve it, not write a second set of replies, and must not re-run the Handlers.
     */
    @Test
    void commitThatLandedButReportedFailedIsNotWrittenTwiceAndHandlersAreNotRerun() {
        Ids ids = Ids.fresh();
        FaultInjection fault = new FaultInjection();
        fault.failAfterNextCommits.set(1);
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("uo", N);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                }, fault.beans(ids.group()))) {
            List<Rep> replies = awaitReplies(ids.replies(), N, WAIT, Duration.ofSeconds(3));

            assertThat(fault.commitCalls.get()).as("the unknown outcome was retried").isGreaterThanOrEqualTo(2);
            assertThat(fault.realCommits.get()).as("replies written to Kafka once").isEqualTo(1);
            assertThat(fault.alerts).as("a destination pause was raised for the unknown outcome").isNotEmpty();
            assertThat(handled.count()).as("Handler invocations").isEqualTo(N);
            assertThat(handled.perCorrelation().values()).containsOnly(1);
            assertReconciled(requested, replies);
            await("worker running again", Duration.ofSeconds(5), () -> w.state() == 0.0);
        }
    }
}
