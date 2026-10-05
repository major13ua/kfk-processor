package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import xme.common.kfkprocessor.requestreply.engine.WorkerState;

/**
 * AC-17 stall indicator and AC-09 permission revoked while a Cycle is in progress, on the real platform.
 * <p>The revocation is injected at the sink ({@link FaultInjection#permissionDenied}), not as a broker ACL change:
 * a broker with an authorizer and SASL users is not part of the suite. The mapping from the broker's authorization
 * errors to the permission fault is covered by {@code KafkaReplySinkTest} and {@code DefaultKafkaDestinationProbeTest}.
 */
@Testcontainers
class StallAndRevokedPermissionIT {

    // AC-17
    @Test
    void stuckHandlerWithPendingWorkRaisesStallAfterTheThresholdIdleWorkerDoesNotAndItClearsAfterTheCommit() {
        Ids ids = Ids.fresh();
        Map<String, Object> p = workerProps(ids, 1000, 100, Duration.ofSeconds(8), Duration.ofSeconds(20));
        p.put("xme.request-reply.stall-threshold", "2s");
        CountDownLatch started = new CountDownLatch(1);

        try (Worker w = startWorker(p, (ctx, req) -> {
            started.countDown();
            Thread.sleep(60_000); // stuck until the 8 s handler timeout cancels it
            return "late";
        })) {
            pause(Duration.ofSeconds(3)); // idle for longer than the threshold
            assertThat(w.state()).as("idle worker raises no stall").isEqualTo(0.0);

            produce(ids.requests(), List.of("stuck-1"));
            await("Handler started", WAIT, () -> started.getCount() == 0);
            assertThat(w.state()).as("pending work within the threshold is not a stall").isEqualTo(0.0);

            await("stall indicator raised", Duration.ofSeconds(5), () -> w.state() == 2.0);

            List<Rep> replies = awaitReplies(ids.replies(), 1, WAIT, Duration.ofSeconds(1));
            assertThat(replies.get(0).isErrorReply()).isTrue();
            await("stall cleared after the commit", Duration.ofSeconds(5), () -> w.state() == 0.0);
        }
    }

    // AC-09 (mid-Cycle)
    @Test
    void permissionRevokedWhileTheCycleRunsPausesWithoutRerunningHandlersAndResumesWhenRestored() {
        Ids ids = Ids.fresh();
        FaultInjection fault = new FaultInjection();
        HandlerLog handled = new HandlerLog();
        CountDownLatch release = new CountDownLatch(1);
        int n = 3;
        List<String> requested = correlationIds("pr", n);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(10), Duration.ofSeconds(30)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    release.await();
                    return "pong:" + req;
                }, fault.beans(ids.group()))) {
            await("Cycle in progress", WAIT, () -> handled.count() >= n);
            fault.permissionDenied.set(true); // revoked while Handlers are running
            release.countDown();

            await("worker paused", Duration.ofSeconds(10), () -> w.state() == 1.0);
            assertThat(fault.alerts).extracting(a -> a.reason()).contains(WorkerState.PauseReason.PERMISSION);
            pause(Duration.ofSeconds(2));
            assertThat(w.state()).as("still paused").isEqualTo(1.0);
            assertThat(handled.count()).as("Cycle not re-executed").isEqualTo(n);
            assertThat(readCommitted(ids.replies(), Duration.ofSeconds(1))).as("nothing committed").isEmpty();
            assertThat(inGroup(ids.group(), ids.identity())).as("stays in its group").isTrue();

            fault.permissionDenied.set(false);

            List<Rep> replies = awaitReplies(ids.replies(), n, RESUME_WITHIN, Duration.ofSeconds(3));
            assertReconciled(requested, replies);
            assertThat(handled.perCorrelation().values()).containsOnly(1);
            await("worker running again", Duration.ofSeconds(5), () -> w.state() == 0.0);
        }
    }
}
