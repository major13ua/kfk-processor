package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import xme.common.kfkprocessor.requestreply.engine.WorkerState;

/** AC-08b: reply destination unavailable. */
@Testcontainers
class DestinationUnavailableIT {

    private static final int N = 3;

    @Test
    void destinationDownPausesWorkerAlertsStaysInGroupThenResumesAndRecommitsWithoutRerunningHandlers() {
        Ids ids = Ids.fresh();
        FaultInjection fault = new FaultInjection();
        fault.destinationDown.set(true);
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("du", N);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                }, fault.beans(ids.group()))) {
            await("Handlers ran for the Cycle", WAIT, () -> handled.count() >= N);
            await("worker paused", Duration.ofSeconds(10), () -> w.state() == 1.0);
            assertThat(fault.alerts).as("Operator alert").extracting(a -> a.reason())
                    .contains(WorkerState.PauseReason.DESTINATION);

            pause(Duration.ofSeconds(2)); // stay paused for several probe intervals
            assertThat(w.state()).as("still paused").isEqualTo(1.0);
            assertThat(inGroup(ids.group(), ids.identity())).as("stays in its group while paused").isTrue();
            assertThat(readCommitted(ids.replies(), Duration.ofSeconds(1))).as("nothing committed while down").isEmpty();
            assertThat(handled.count()).as("Handlers not re-run while paused").isEqualTo(N);

            fault.destinationDown.set(false);

            List<Rep> replies = awaitReplies(ids.replies(), N, RESUME_WITHIN, Duration.ofSeconds(3));
            assertThat(w.state()).as("resumed automatically").isEqualTo(0.0);
            assertThat(inGroup(ids.group(), ids.identity())).as("still in group after resume").isTrue();
            assertThat(handled.count()).as("Handler invocations after re-commit").isEqualTo(N);
            assertThat(handled.perCorrelation().values()).containsOnly(1);
            assertReconciled(requested, replies);
        }
    }
}
