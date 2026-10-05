package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Review r2 finding A1 (task F12): a Cycle held on a failing commit survives a rebalance. The held member rejoins
 * through its keep-alive poll, the Eager assignor resets the position to the committed offset, so after the held
 * commit the already-answered requests are fetched again and the Handlers re-run (or the partition moved and a
 * second member answers them again). Expected: Handlers run once per request across all members, exactly one
 * committed reply per request.
 */
@Testcontainers
class RebalanceWhileHeldIT {

    private static final int N = 5;

    @Test
    void rebalanceWhileACycleIsHeldDoesNotRerunHandlersNorDuplicateReplies() {
        Ids ids = Ids.fresh();
        Ids idsB = new Ids(ids.requests(), ids.replies(), ids.group(), ids.identity() + "-b");
        FaultInjection faultA = new FaultInjection();
        faultA.failNextCommits.set(1000); // the Cycle stays held
        FaultInjection faultB = new FaultInjection();
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("rb", N);
        produce(ids.requests(), requested);

        try (Worker a = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                }, faultA.beans(ids.group()))) {
            await("Handlers ran for the Cycle", WAIT, () -> handled.count() >= N);
            await("worker A paused with the Cycle held", Duration.ofSeconds(15), () -> a.state() == 1.0);

            try (Worker b = startWorker(workerProps(idsB, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                    (ctx, req) -> {
                        handled.record(ctx.correlationId(), ctx.idempotencyKey());
                        return "pong-b:" + req;
                    }, faultB.beans(ids.group()))) {
                await("second member joined the group", Duration.ofSeconds(30), () -> members(ids.group()) == 2);
                pause(Duration.ofSeconds(5)); // rebalance settles while A keeps its held Cycle

                faultA.failNextCommits.set(0);

                List<Rep> replies = awaitReplies(ids.replies(), N, RESUME_WITHIN, Duration.ofSeconds(8));
                assertThat(handled.count()).as("Handler invocations across both members").isEqualTo(N);
                assertThat(handled.perCorrelation().values()).as("each request run once").containsOnly(1);
                assertReconciled(requested, replies);
            }
        }
    }

    private static int members(String group) {
        try (Admin admin = admin()) {
            return admin.describeConsumerGroups(List.of(group)).describedGroups().get(group)
                    .get(10, TimeUnit.SECONDS).members().size();
        } catch (Exception e) {
            return -1;
        }
    }
}
