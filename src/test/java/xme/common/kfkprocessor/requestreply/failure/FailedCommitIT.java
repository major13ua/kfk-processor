package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** AC-05 and QG-2: a failed Cycle commit is retried with the same results; Handlers are never re-run. */
@Testcontainers(disabledWithoutDocker = true)
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
        fault.failNextCommits.set(3); // default commit-retry-attempts is 3: the Cycle is held
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("fx", N);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                }, fault.beans(ids.group()))) {
            List<Rep> replies = awaitReplies(ids.replies(), N, WAIT, Duration.ofSeconds(3));

            assertThat(fault.commitCalls.get()).as("3 failed attempts then the re-commit").isGreaterThanOrEqualTo(4);
            assertThat(handled.count()).as("Handler invocations").isEqualTo(N);
            assertThat(handled.perCorrelation().values()).containsOnly(1);
            assertReconciled(requested, replies);
            assertThat(w.state()).as("worker running again").isEqualTo(0.0);
        }
    }
}
