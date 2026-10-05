package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Worker-level error paths on the real platform: AC-06 (handler failure), AC-07 (timeout and cancel), AC-08 (oversized
 * and broker-rejected reply), AC-10b (no allowance, malformed request), AC-13 (slow low-weight lane). The reply that
 * cannot be encoded is not reachable here: the starter's default codec is {@code String.valueOf(data)}, so that path
 * is covered by {@code CycleCommitterTest} and {@code KafkaReplySinkTest}.
 */
@Testcontainers
class ErrorPathsIT {

    private static final Duration HANDLER_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration COMMIT_WINDOW = Duration.ofSeconds(20);

    private static Map<String, Object> props(Ids ids, long budget) {
        return workerProps(ids, budget, 100, HANDLER_TIMEOUT, COMMIT_WINDOW);
    }

    private static boolean body(Rep r, String category) {
        return r.body() != null && r.body().contains("\"category\":\"" + category + "\"");
    }

    // AC-06, AC-07, AC-07b
    @Test
    void failingAndTimingOutHandlersGetErrorRepliesWithTheirCategoryAndSiblingsGetNormalRepliesEachHandlerRunsOnce() {
        Ids ids = Ids.fresh();
        HandlerLog handled = new HandlerLog();
        AtomicInteger cancelled = new AtomicInteger();
        List<String> requested = List.of("ok-1", "bad-1", "slow-1", "ok-2", "ok-3");
        produce(ids.requests(), requested);

        try (Worker w = startWorker(props(ids, 1000), (ctx, req) -> {
            handled.record(ctx.correlationId(), ctx.idempotencyKey());
            if (ctx.correlationId().startsWith("bad")) {
                throw new IllegalStateException("boom with internal detail");
            }
            if (ctx.correlationId().startsWith("slow")) {
                try {
                    while (!ctx.cancellation().isCancelled()) {
                        Thread.sleep(20);
                    }
                } catch (InterruptedException interrupted) {
                    // cancellation may arrive as an interrupt
                }
                cancelled.incrementAndGet();
                return "late";
            }
            return "pong:" + req;
        })) {
            List<Rep> replies = awaitReplies(ids.replies(), requested.size(), WAIT, Duration.ofSeconds(3));

            assertReconciled(requested, replies);
            Map<String, Rep> byId = new java.util.HashMap<>();
            replies.forEach(r -> byId.put(r.correlationId(), r));
            assertThat(byId.get("bad-1").isErrorReply()).isTrue();
            assertThat(body(byId.get("bad-1"), "failure")).as(byId.get("bad-1").body()).isTrue();
            assertThat(byId.get("bad-1").body()).as("no internal detail in the Error Reply").doesNotContain("boom");
            assertThat(byId.get("slow-1").isErrorReply()).isTrue();
            assertThat(body(byId.get("slow-1"), "timeout")).as(byId.get("slow-1").body()).isTrue();
            for (String ok : List.of("ok-1", "ok-2", "ok-3")) {
                assertThat(byId.get(ok).type()).as(ok + " is a normal reply, wire type").isEqualTo("reply");
                assertThat(byId.get(ok).body()).isEqualTo("pong:req-" + ok);
            }
            assertThat(cancelled.get()).as("cancel signal reached the timed-out Handler").isEqualTo(1);
            assertThat(handled.perCorrelation().values()).as("each Handler ran once, failures not retried")
                    .containsOnly(1).hasSize(requested.size());
            assertThat(w.errorReplies("FAILURE")).isEqualTo(1);
            assertThat(w.errorReplies("TIMEOUT")).isEqualTo(1);
        }
    }

    // AC-08: reply larger than the producer limit
    @Test
    void oversizedReplyBecomesUndeliverableErrorReplyOthersCommitAndHandlersAreNotRerun() {
        Ids ids = Ids.fresh();
        HandlerLog handled = new HandlerLog();
        List<String> requested = List.of("ok-1", "big-1", "ok-2");
        produce(ids.requests(), requested);

        try (Worker w = startWorker(props(ids, 1000), (ctx, req) -> {
            handled.record(ctx.correlationId(), ctx.idempotencyKey());
            return ctx.correlationId().startsWith("big") ? "x".repeat(2 * 1024 * 1024) : "pong:" + req;
        })) {
            List<Rep> replies = awaitReplies(ids.replies(), requested.size(), WAIT, Duration.ofSeconds(3));

            assertUndeliverableOnlyForBig(requested, replies, handled);
        }
    }

    // AC-08: reply the broker rejects on send (topic limit below the reply size, below the client limit)
    @Test
    void replyRejectedByTheBrokerBecomesUndeliverableErrorReplyOthersCommitAndHandlersAreNotRerun() throws Exception {
        Ids ids = Ids.fresh();
        try (var admin = admin()) {
            var topic = new ConfigResource(ConfigResource.Type.TOPIC, ids.replies());
            admin.incrementalAlterConfigs(Map.of(topic, List.of(new AlterConfigOp(
                    new ConfigEntry("max.message.bytes", "2000"), AlterConfigOp.OpType.SET)))).all()
                    .get(30, TimeUnit.SECONDS);
        }
        HandlerLog handled = new HandlerLog();
        List<String> requested = List.of("ok-1", "big-1", "ok-2");
        produce(ids.requests(), requested);

        try (Worker w = startWorker(props(ids, 1000), (ctx, req) -> {
            handled.record(ctx.correlationId(), ctx.idempotencyKey());
            return ctx.correlationId().startsWith("big") ? "x".repeat(50_000) : "pong:" + req;
        })) {
            List<Rep> replies = awaitReplies(ids.replies(), requested.size(), WAIT, Duration.ofSeconds(3));

            assertUndeliverableOnlyForBig(requested, replies, handled);
        }
    }

    private static void assertUndeliverableOnlyForBig(List<String> requested, List<Rep> replies, HandlerLog handled) {
        assertReconciled(requested, replies);
        for (Rep r : replies) {
            if (r.correlationId().startsWith("big")) {
                assertThat(r.isErrorReply()).as("big reply replaced by an Error Reply").isTrue();
                assertThat(body(r, "undeliverable")).as(r.body()).isTrue();
            } else {
                assertThat(r.type()).as(r.correlationId() + " committed normally, wire type").isEqualTo("reply");
                assertThat(r.body()).startsWith("pong:");
            }
        }
        assertThat(handled.count()).as("Handlers not re-run").isEqualTo(requested.size());
        assertThat(handled.perCorrelation().values()).containsOnly(1);
    }

    // AC-10b: malformed request is answered without using allowance
    @Test
    void malformedRequestsGetErrorRepliesWithoutUsingAnyAllowance() {
        Ids ids = Ids.fresh();
        int n = 30;
        try (var producer = rawProducer()) {
            for (int i = 0; i < n; i++) { // no correlation_id header
                var rec = new ProducerRecord<byte[], byte[]>(ids.requests(), null, ("m" + i).getBytes(StandardCharsets.UTF_8));
                rec.headers().add(new RecordHeader("request_key", ("k" + i).getBytes(StandardCharsets.UTF_8)));
                producer.send(rec);
            }
            producer.flush();
        }
        AtomicInteger handled = new AtomicInteger();

        try (Worker w = startWorker(props(ids, 20), (ctx, req) -> {
            handled.incrementAndGet();
            return "pong";
        })) {
            List<Rep> replies = awaitReplies(ids.replies(), n, WAIT, Duration.ofSeconds(2));

            assertThat(replies).hasSize(n).allSatisfy(r -> assertThat(r.isErrorReply()).isTrue());
            assertThat(handled.get()).as("no Handler ran").isZero();
            assertThat(sumAccepted(w)).as("no allowance unit was taken").isZero();
        }
    }

    // AC-10b: without allowance the requests stay on the platform, unanswered, and a later Cycle serves them
    @Test
    void requestsBeyondTheBudgetStayOnThePlatformUnansweredAndAreServedLaterNothingLost() {
        Ids ids = Ids.fresh();
        int n = 100;
        HandlerLog handled = new HandlerLog();
        List<String> requested = correlationIds("bg", n);
        produce(ids.requests(), requested);

        try (Worker w = startWorker(props(ids, 20), (ctx, req) -> {
            handled.record(ctx.correlationId(), ctx.idempotencyKey());
            return "pong:" + req;
        })) {
            await("first requests accepted", WAIT, () -> handled.count() > 0);
            pause(Duration.ofSeconds(2));
            int early = readCommitted(ids.replies(), Duration.ofMillis(500)).size();
            assertThat(early).as("only the budget's worth is answered early").isLessThan(n);
            assertThat(handled.count()).as("the rest is not consumed or handled yet").isLessThan(n);

            List<Rep> replies = awaitReplies(ids.replies(), n, WAIT, Duration.ofSeconds(3));
            assertReconciled(requested, replies);
            assertThat(handled.perCorrelation().values()).containsOnly(1);
        }
    }

    // AC-13: a slow Handler on the low-weight lane does not hold requests past the Cycle deadline. The slow requests
    // share ONE Request Key, so all but the first wait queued behind it and are ended by the Cycle-deadline cut, not
    // by their own Handler timeout (they are never dispatched).
    @Test
    void slowHandlersOnTheLowWeightLaneDoNotHoldRequestsPastTheCycleDeadline() throws Exception {
        Ids ids = Ids.fresh();
        String lowTopic = "low-" + ids.requests();
        try (var admin = admin()) {
            admin.createTopics(List.of(new NewTopic(lowTopic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
            awaitTopicReady(admin, lowTopic);
        }
        Duration deadline = Duration.ofSeconds(3);
        Map<String, Object> p = workerProps(ids, 1000, 100, deadline, COMMIT_WINDOW);
        p.put("xme.request-reply.cycle-deadline", deadline.toMillis() + "ms");
        p.put("xme.request-reply.lanes[0].name", "high");
        p.put("xme.request-reply.lanes[0].weight", "9");
        p.put("xme.request-reply.lanes[1].name", "low");
        p.put("xme.request-reply.lanes[1].source", lowTopic);
        p.put("xme.request-reply.lanes[1].weight", "1");
        List<String> fast = correlationIds("fast", 20);
        List<String> slow = correlationIds("slow", 5);
        produce(ids.requests(), fast);
        produce(lowTopic, slow, id -> "slow-key");
        HandlerLog handled = new HandlerLog();

        try (Worker w = startWorker(p, (ctx, req) -> {
            handled.record(ctx.correlationId(), ctx.idempotencyKey());
            if (ctx.correlationId().startsWith("slow")) {
                Thread.sleep(60_000); // far beyond the deadline
            }
            return "pong:" + req;
        })) {
            await("Handlers started", WAIT, () -> handled.count() >= fast.size() + 1);
            long startedAt = System.nanoTime();
            List<String> all = new java.util.ArrayList<>(fast);
            all.addAll(slow);
            List<Rep> replies = awaitReplies(ids.replies(), all.size(), WAIT, Duration.ofSeconds(2));
            long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt) - 2000; // minus the settle

            assertReconciled(all, replies);
            assertThat(waitedMs).as("all replies committed within the deadline of the Cycle (plus commit and read slack)")
                    .isLessThan(deadline.toMillis() + 3000);
            for (Rep r : replies) {
                if (r.correlationId().startsWith("slow")) {
                    assertThat(body(r, "timeout")).as(r.correlationId() + " " + r.body()).isTrue();
                } else {
                    assertThat(r.type()).as(r.correlationId() + " answered normally, wire type").isEqualTo("reply");
                }
            }
            assertThat(handled.perCorrelation().values()).containsOnly(1);
            assertThat(handled.perCorrelation().keySet().stream().filter(c -> c.startsWith("slow")))
                    .as("only the head of the same-key chain started; the queued ones were cut by the Cycle deadline")
                    .containsExactly("slow-0");
        }
    }

    private static double sumAccepted(Worker w) {
        return w.registry.find("requestreply.accepted").counters().stream().mapToDouble(c -> c.count()).sum();
    }

    private static KafkaProducer<byte[], byte[]> rawProducer() {
        return new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 120_000));
    }
}
