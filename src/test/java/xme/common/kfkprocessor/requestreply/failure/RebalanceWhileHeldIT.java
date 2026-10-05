package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    /**
     * Review r3 Group C: two partitions, so the second member really takes one over. Requests of the retained
     * partition run once; requests of the moved partition run twice (A before the move, B after: A's Cycle was never
     * committed); exactly one committed reply per request either way (A drops the replies of the revoked partition).
     */
    @Test
    void rebalanceWhileHeldOnTwoPartitionsRunsOnlyMovedPartitionRequestsTwiceAndCommitsOneReplyEach() {
        Ids ids = Ids.fresh(2);
        Ids idsB = new Ids(ids.requests(), ids.replies(), ids.group(), ids.identity() + "-b");
        FaultInjection faultA = new FaultInjection();
        faultA.failNextCommits.set(1000);
        FaultInjection faultB = new FaultInjection();
        HandlerLog handledA = new HandlerLog();
        HandlerLog handledB = new HandlerLog();
        List<String> requested = correlationIds("rb2", 2 * N);
        java.util.function.Function<String, Integer> partitionOf =
                id -> Integer.parseInt(id.substring(id.lastIndexOf('-') + 1)) % 2;
        produce(ids.requests(), requested, id -> id, partitionOf);

        try (Worker a = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handledA.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                }, faultA.beans(ids.group()))) {
            await("Handlers ran for the Cycle", WAIT, () -> handledA.count() >= 2 * N);
            await("worker A paused with the Cycle held", Duration.ofSeconds(15), () -> a.state() == 1.0);

            try (Worker b = startWorker(workerProps(idsB, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                    (ctx, req) -> {
                        handledB.record(ctx.correlationId(), ctx.idempotencyKey());
                        return "pong-b:" + req;
                    }, faultB.beans(ids.group()))) {
                await("second member joined the group", Duration.ofSeconds(30), () -> members(ids.group()) == 2);
                pause(Duration.ofSeconds(5));

                faultA.failNextCommits.set(0);

                List<Rep> replies = awaitReplies(ids.replies(), 2 * N, RESUME_WITHIN, Duration.ofSeconds(8));
                assertReconciled(requested, replies);

                Set<Integer> movedPartitions = new HashSet<>();
                handledB.perCorrelation().keySet().forEach(id -> movedPartitions.add(partitionOf.apply(id)));
                assertThat(movedPartitions).as("B took over exactly one partition").hasSize(1);
                int moved = movedPartitions.iterator().next();
                assertThat(handledA.perCorrelation().values()).as("A ran each request once").containsOnly(1);
                assertThat(handledA.count()).isEqualTo(2 * N);
                assertThat(handledB.perCorrelation().values()).as("B ran each moved request once").containsOnly(1);
                assertThat(handledB.perCorrelation().keySet()).as("B ran exactly the requests of the moved partition")
                        .containsExactlyInAnyOrderElementsOf(
                                requested.stream().filter(id -> partitionOf.apply(id) == moved).toList());
                // total runs: every request once, the moved partition's requests twice
                assertThat(handledA.count() + handledB.count()).isEqualTo(2 * N + N);
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
