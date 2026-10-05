package xme.common.kfkprocessor.requestreply.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;

/**
 * Each test creates its own topics, group and identity, so tests share only the (never destroyed) broker.
 * Readers use read_committed, as Requesters must (ADR-0002).
 */
@Testcontainers
class KafkaReplySinkIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    private record Env(String group, String identity, String requests, String replies) {
        static Env create() throws Exception {
            return create(null);
        }

        /** A reply topic whose broker-side limit is {@code maxMessageBytes} (null = broker default). */
        static Env create(Integer maxMessageBytes) throws Exception {
            String id = UUID.randomUUID().toString().substring(0, 8);
            var env = new Env("grp-" + id, "worker-" + id, "req-" + id, "rep-" + id);
            try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(env.requests, 2, (short) 1),
                        new NewTopic(env.replies, 1, (short) 1).configs(maxMessageBytes == null ? Map.of()
                                : Map.of("max.message.bytes", maxMessageBytes.toString())))).all().get();
            }
            return env;
        }

        KafkaReplySink sink() {
            var p = new RequestReplyProperties();
            p.setWorkerIdentity(identity);
            p.setReplyDestination(replies);
            p.setCommitWindow(Duration.ofSeconds(15));
            var lane = new RequestReplyProperties.Lane();
            lane.setName("high");
            lane.setSource(requests);
            lane.setWeight(1);
            p.setLanes(List.of(lane));
            return new KafkaReplySink(p, kafka.getBootstrapServers(), group);
        }

        ReplyRecord reply(int partition, long position, String corr, byte[] value) {
            return new ReplyRecord("high", partition, position, corr, "key-" + corr, value, false);
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Properties producerProps(String txId) {
        var p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        return p;
    }

    /** Correlation ids a read_committed Requester sees on the reply topic, polling for {@code observe}. */
    private static List<String> committedReplies(Env env, Duration observe) {
        var p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        var seen = new ArrayList<String>();
        try (var c = new KafkaConsumer<byte[], byte[]>(p)) {
            c.subscribe(List.of(env.replies));
            long end = System.nanoTime() + observe.toNanos();
            while (System.nanoTime() < end) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(200))) {
                    seen.add(new String(r.headers().lastHeader("correlation_id").value(), StandardCharsets.UTF_8));
                }
            }
        }
        return seen;
    }

    private static Map<TopicPartition, OffsetAndMetadata> groupOffsets(Env env) throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            return admin.listConsumerGroupOffsets(env.group).partitionsToOffsetAndMetadata().get();
        }
    }

    /** A Cycle killed before its commit: reply and positions written into an open transaction, then abandoned. */
    private static KafkaProducer<byte[], byte[]> abandonedCycle(Env env, String corr, long position) {
        var tx = new KafkaProducer<byte[], byte[]>(producerProps(KafkaReplySink.transactionalId(env.group, env.identity)));
        tx.initTransactions();
        tx.beginTransaction();
        var r = new ProducerRecord<byte[], byte[]>(env.replies, null, b("dead"));
        r.headers().add(new RecordHeader("correlation_id", b(corr)));
        tx.send(r);
        tx.sendOffsetsToTransaction(
                Map.of(new TopicPartition(env.requests, 0), new OffsetAndMetadata(position + 1)),
                new org.apache.kafka.clients.consumer.ConsumerGroupMetadata(env.group, -1, "", java.util.Optional.empty()));
        tx.flush();
        return tx; // never committed, never closed: the process "died"
    }

    // AC-05
    @Test
    void cycleKilledBeforeCommitAndRerunYieldsExactlyOneCommittedReply() throws Exception {
        var env = Env.create();
        var dead = abandonedCycle(env, "c-1", 0);
        try (var sink = env.sink()) {
            assertTrue(committedReplies(env, Duration.ofSeconds(2)).isEmpty(), "abandoned reply is visible");
            assertTrue(groupOffsets(env).isEmpty(), "abandoned position is committed");

            sink.commit(List.of(env.reply(0, 0, "c-1", b("answer"))));
        } finally {
            dead.close(Duration.ZERO);
        }
        assertEquals(List.of("c-1"), committedReplies(env, Duration.ofSeconds(5)));
        assertEquals(1L, groupOffsets(env).get(new TopicPartition(env.requests, 0)).offset());
    }

    // AC-05 (a fully repeated, already-committed Cycle by a restarted worker adds no visible duplicate)
    @Test
    void restartedWorkerWithSameIdentityKeepsEarlierCommittedReplyUnique() throws Exception {
        var env = Env.create();
        try (var first = env.sink()) {
            first.commit(List.of(env.reply(0, 0, "c-1", b("answer"))));
        }
        // after restart the request is not re-fetched (position committed); a new request gets its own reply
        try (var second = env.sink()) {
            second.commit(List.of(env.reply(0, 1, "c-2", b("answer-2"))));
        }
        var seen = committedReplies(env, Duration.ofSeconds(5));
        assertEquals(List.of("c-1", "c-2"), seen.stream().sorted().toList());
        assertEquals(2L, groupOffsets(env).get(new TopicPartition(env.requests, 0)).offset());
    }

    // AC-05 (replies and positions commit atomically, headers carried)
    @Test
    void commitMakesRepliesAndPositionsVisibleTogether() throws Exception {
        var env = Env.create();
        try (var sink = env.sink()) {
            CommitResult r = sink.commit(List.of(
                    env.reply(0, 4, "c-1", b("r1")),
                    env.reply(0, 5, "c-2", b("r2")),
                    env.reply(1, 7, "c-3", b("r3"))));
            assertTrue(r.failures().isEmpty());
        }
        assertEquals(List.of("c-1", "c-2", "c-3"), committedReplies(env, Duration.ofSeconds(5)).stream().sorted().toList());
        var offsets = groupOffsets(env);
        assertEquals(6L, offsets.get(new TopicPartition(env.requests, 0)).offset());
        assertEquals(8L, offsets.get(new TopicPartition(env.requests, 1)).offset());
    }

    // AC-05 (too large reply is a per-request result, the rest of the Cycle commits)
    @Test
    void tooLargeReplyDoesNotAbortTheCycle() throws Exception {
        var env = Env.create();
        CommitResult r;
        try (var sink = env.sink()) {
            r = sink.commit(List.of(
                    env.reply(0, 0, "ok-1", b("a")),
                    env.reply(0, 1, "big", new byte[3 * 1024 * 1024]),
                    env.reply(0, 2, "ok-2", b("b"))));
        }
        assertEquals(1, r.failures().size());
        assertEquals("big", r.failures().get(0).correlationId());
        assertEquals(CommitResult.Reason.TOO_LARGE, r.failures().get(0).reason());
        assertEquals(List.of("ok-1", "ok-2"), committedReplies(env, Duration.ofSeconds(5)).stream().sorted().toList());
        assertEquals(3L, groupOffsets(env).get(new TopicPartition(env.requests, 0)).offset());
    }

    /** correlation id to value of every committed reply record a read_committed Requester sees. */
    private static List<String> committedRepliesWithValues(Env env, Duration observe) {
        var p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        var seen = new ArrayList<String>();
        try (var c = new KafkaConsumer<byte[], byte[]>(p)) {
            c.subscribe(List.of(env.replies));
            long end = System.nanoTime() + observe.toNanos();
            while (System.nanoTime() < end) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(200))) {
                    seen.add(new String(r.headers().lastHeader("correlation_id").value(), StandardCharsets.UTF_8)
                            + "=" + new String(r.value(), StandardCharsets.UTF_8));
                }
            }
        }
        return seen;
    }

    // AC-08 (the undeliverable Error Reply replaces the too large reply in the same transaction)
    @Test
    void tooLargeReplyIsReplacedByItsUndeliverableErrorReplyInTheSameTransaction() throws Exception {
        var env = Env.create();
        CommitResult r;
        try (var sink = env.sink()) {
            r = sink.commit(List.of(
                    env.reply(0, 0, "ok-1", b("a")),
                    new ReplyRecord("high", 0, 1, "big", "key-big", new byte[3 * 1024 * 1024], false,
                            b("undeliverable")),
                    env.reply(0, 2, "ok-2", b("b"))));
        }
        assertEquals(1, r.failures().size());
        assertTrue(r.failures().get(0).substituted());
        assertEquals(List.of("big=undeliverable", "ok-1=a", "ok-2=b"),
                committedRepliesWithValues(env, Duration.ofSeconds(5)).stream().sorted().toList());
        assertEquals(3L, groupOffsets(env).get(new TopicPartition(env.requests, 0)).offset());
    }

    // AC-08 (review A1: the broker, not the client, rejects the reply; the failed send poisons the transaction)
    @Test
    void replyRejectedByTheBrokerIsReplacedByItsUndeliverableErrorReply() throws Exception {
        var env = Env.create(2000);
        CommitResult r;
        try (var sink = env.sink()) {
            r = sink.commit(List.of(
                    env.reply(0, 0, "ok-1", b("a")),
                    new ReplyRecord("high", 0, 1, "big", "key-big", new byte[100 * 1024], false, b("undeliverable")),
                    env.reply(0, 2, "ok-2", b("b"))));
        }
        assertEquals(1, r.failures().size());
        assertTrue(r.failures().get(0).substituted());
        assertEquals(List.of("big=undeliverable", "ok-1=a", "ok-2=b"),
                committedRepliesWithValues(env, Duration.ofSeconds(5)).stream().sorted().toList());
        assertEquals(3L, groupOffsets(env).get(new TopicPartition(env.requests, 0)).offset());
    }

    // AC-08 (review A1: an Error Reply the broker rejects is reported, the Cycle and its positions still commit)
    @Test
    void errorReplyRejectedByTheBrokerIsReportedAndTheCycleCommits() throws Exception {
        var env = Env.create(2000);
        CommitResult r;
        try (var sink = env.sink()) {
            r = sink.commit(List.of(
                    env.reply(0, 0, "ok-1", b("a")),
                    new ReplyRecord("high", 0, 1, "err", "key-err", new byte[100 * 1024], true, null)));
        }
        assertEquals(1, r.failures().size());
        assertEquals(List.of("ok-1=a"), committedRepliesWithValues(env, Duration.ofSeconds(5)));
        assertEquals(2L, groupOffsets(env).get(new TopicPartition(env.requests, 0)).offset());
    }

    private static final String PAD = "x".repeat(3000);

    // AC-08 (review A4: undrained sends fail with the oversized reply's exception; only that reply is demoted)
    @Test
    void oneBrokerRejectedReplyAmongManyDemotesOnlyThatReply() throws Exception {
        var env = Env.create(8000); // followers of 3 KB: many batches, more than max.in.flight, so some are still undrained when the first is rejected
        var replies = new ArrayList<ReplyRecord>();
        replies.add(new ReplyRecord("high", 0, 0, "big", "key-big", new byte[9000], false, b("undeliverable")));
        for (int i = 1; i <= 50; i++) {
            replies.add(new ReplyRecord("high", 0, i, "ok-" + i, "key-ok-" + i, b("v" + i + PAD), false,
                    b("undeliverable-ok-" + i)));
        }
        CommitResult r;
        try (var sink = env.sink()) {
            r = sink.commit(replies);
        }
        assertEquals(List.of("big"), r.failures().stream().map(CommitResult.ReplyFailure::correlationId).toList(),
                "only the rejected reply is a failure");
        var expected = new ArrayList<String>();
        expected.add("big=undeliverable");
        for (int i = 1; i <= 50; i++) {
            expected.add("ok-" + i + "=v" + i + PAD);
        }
        var committed = committedRepliesWithValues(env, Duration.ofSeconds(8));
        assertEquals(51, committed.size(), "one committed reply per request");
        assertEquals(expected.stream().sorted().toList(), committed.stream().sorted().toList());
        assertEquals(51L, groupOffsets(env).get(new TopicPartition(env.requests, 0)).offset());
    }

    private static final int AHEAD = 40;

    // AC-08, review r3 Group C: the rejected reply sits mid-batch, not at index 0. Spec section 8 (F14 residual)
    // allows good replies ahead of it to be answered with an undeliverable Error Reply; on this broker the producer
    // splits the rejected batch and resends, so they are delivered. Either way the invariants below hold: one
    // committed reply per request, every reported failure matches what the Requester sees, positions committed.
    @Test
    void midBatchBrokerRejectedReplyKeepsOneCommittedReplyPerRequestAndReportsExactlyTheSubstitutedOnes() throws Exception {
        var env = Env.create(8000);
        var replies = new ArrayList<ReplyRecord>();
        for (int i = 0; i < AHEAD; i++) {
            replies.add(new ReplyRecord("high", 0, i, "ok-" + i, "key-ok-" + i, b("v" + i + PAD), false,
                    b("undeliverable-ok-" + i)));
        }
        replies.add(new ReplyRecord("high", 0, AHEAD, "big", "key-big", new byte[9000], false, b("undeliverable-big")));
        for (int i = AHEAD + 1; i < AHEAD + 11; i++) {
            replies.add(new ReplyRecord("high", 0, i, "ok-" + i, "key-ok-" + i, b("v" + i + PAD), false,
                    b("undeliverable-ok-" + i)));
        }
        CommitResult r;
        try (var sink = env.sink()) {
            r = sink.commit(replies);
        }
        var failed = r.failures().stream().map(CommitResult.ReplyFailure::correlationId).toList();
        assertTrue(failed.contains("big"), "the rejected reply is a failure: " + failed);
        var expected = new ArrayList<String>();
        for (ReplyRecord rep : replies) {
            boolean substituted = failed.contains(rep.correlationId());
            expected.add(rep.correlationId() + "=" + new String(substituted ? rep.fallback() : rep.value(),
                    StandardCharsets.UTF_8));
        }
        var committed = committedRepliesWithValues(env, Duration.ofSeconds(8));
        assertEquals(replies.size(), committed.size(), "one committed reply per request");
        assertEquals(expected.stream().sorted().toList(), committed.stream().sorted().toList(),
                "what the Requesters see matches the reported failures");
        assertEquals(List.of("big"), failed, "only the rejected reply is substituted (good replies ahead of it survive)");
        assertEquals((long) replies.size(), groupOffsets(env).get(new TopicPartition(env.requests, 0)).offset());
    }
}
