package xme.common.kfkprocessor.requestreply.adapters.kafka;

import static xme.common.kfkprocessor.TestcontainersConfiguration.newKafka;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.api.Reply;
import xme.common.kfkprocessor.requestreply.engine.CommitRetry;
import xme.common.kfkprocessor.requestreply.engine.CycleCommitter;
import xme.common.kfkprocessor.requestreply.engine.HandlerResult;
import xme.common.kfkprocessor.requestreply.engine.WorkerState;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * Review r2 A2 (task F13), AC-08b and AC-05: the real sink over a real broker when the commit is held longer than
 * the transaction timeout (= commit window). The broker aborts the transaction and bumps the producer epoch.
 *
 * <p>Changed from the first RED version, which expected the sink to recover in place on an InvalidProducerEpoch.
 * That premise is wrong for kafka-clients 4.2.1: TransactionManager (EndTxnHandler) maps INVALID_PRODUCER_EPOCH to
 * PRODUCER_FENCED, so the commit fails with ProducerFencedException, which the sink cannot tell from a real fence.
 * Human decision (option 3): a fence on commit is permanent: the worker pauses with the {@code fenced} alert and
 * never re-sends (no duplicate replies, AC-05). The restarted sink below (new producer taking the id) only proves
 * producer recovery, not the restart path (a restart re-fetches the requests and re-runs the Handlers); it commits
 * exactly one reply per request with the positions advanced once.
 */
@Testcontainers
class KafkaReplySinkTransactionTimeoutIT {

    private static final Duration TXN_TIMEOUT = Duration.ofSeconds(3);
    private static final int N = 5;

    @Container
    static KafkaContainer kafka = newKafka(k -> k
            // the broker aborts timed-out transactions promptly instead of after the 10 s default sweep
            .withEnv("KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS", "500"));

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A transactional producer that really talks to the broker, but waits {@code hold} before the first commit
     * (everything is already sent and flushed by then). A discarded producer is replaced by a new real one on the
     * next initTransactions, as the production factory would.
     */
    private static Producer<byte[], byte[]> holdingProducer(Properties props, Duration hold, AtomicBoolean held) {
        var delegate = new java.util.concurrent.atomic.AtomicReference<KafkaProducer<byte[], byte[]>>();
        @SuppressWarnings("unchecked")
        Producer<byte[], byte[]> proxy = (Producer<byte[], byte[]>) Proxy.newProxyInstance(
                Producer.class.getClassLoader(), new Class<?>[] {Producer.class}, (p, m, a) -> {
                    if (m.getName().equals("initTransactions") && delegate.get() == null) {
                        delegate.set(new KafkaProducer<>(props));
                    }
                    if (m.getName().equals("close")) {
                        var d = delegate.getAndSet(null);
                        if (d != null) {
                            d.close(Duration.ZERO);
                        }
                        return null;
                    }
                    if (m.getName().equals("commitTransaction") && held.compareAndSet(false, true)) {
                        Thread.sleep(hold.toMillis());
                    }
                    try {
                        return m.invoke(delegate.get(), a);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        return proxy;
    }

    private static CycleCommitter<String, String> committer(KafkaReplySink sink) {
        return new CycleCommitter<>(sink, r -> b(r.data()), e -> b("error|" + e.correlationId()));
    }

    @Test
    void commitHeldLongerThanTheTransactionTimeoutEndsWithOneCommittedReplyPerRequest() throws Exception {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String group = "grp-" + id;
        String requests = "req-" + id;
        String replies = "rep-" + id;
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(requests, 1, (short) 1), new NewTopic(replies, 1, (short) 1)))
                    .all().get();
        }
        var props = new RequestReplyProperties();
        props.setWorkerIdentity("worker-" + id);
        props.setReplyDestination(replies);
        props.setCommitWindow(TXN_TIMEOUT); // transaction.timeout.ms = max.block.ms = commit window
        var lane = new RequestReplyProperties.Lane();
        lane.setName("main");
        lane.setSource(requests);
        lane.setWeight(1);
        props.setLanes(List.of(lane));
        Properties producerProps = KafkaReplySink.producerProperties(props,
                Map.of("bootstrap.servers", kafka.getBootstrapServers()), group);
        assertEquals((int) TXN_TIMEOUT.toMillis(), producerProps.get("transaction.timeout.ms"));

        var held = new AtomicBoolean();
        @SuppressWarnings("removal")
        var groupMeta = new org.apache.kafka.clients.consumer.ConsumerGroupMetadata(group, -1, "",
                java.util.Optional.empty());
        List<HandlerResult<String, String>> cycle = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            var request = new IncomingRequest("main", 0, i, "key-" + i, "c-" + i, Map.of(), b("q-" + i));
            cycle.add(new HandlerResult<>(request, new Reply<>("c-" + i, "key-" + i, "r-" + i), null));
        }

        boolean fenced;
        var alerts = new ArrayList<CommitRetry.Alert>();
        var clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        var state = new WorkerState(clock, Duration.ofSeconds(60), (WorkerMetrics) Proxy.newProxyInstance(
                WorkerMetrics.class.getClassLoader(), new Class<?>[] {WorkerMetrics.class}, (p, m, a) -> null));
        try (var sink = new KafkaReplySink(holdingProducer(producerProps, TXN_TIMEOUT.plusSeconds(6), held), replies,
                Map.of("main", requests), groupMeta)) {
            var retry = new CommitRetry<String, String>(committer(sink), () -> { }, state, clock,
                    Duration.ofSeconds(1), 3, alerts::add);

            // The broker generation decides how the timed-out commit surfaces. Brokers 3.x: INVALID_PRODUCER_EPOCH, which
            // the client reports as ProducerFenced: the commit is held and the worker pauses (permanent fence).
            // Brokers 4.x (KIP-890): INVALID_TXN_STATE, a definitive "not committed": the sink resends and commits.
            // Either way the Cycle is committed once, which the checks below verify.
            fenced = retry.commit(cycle).isEmpty();
            if (fenced) {
                assertTrue(retry.holding());
                assertEquals(WorkerState.Status.PAUSED, state.evaluate());
                assertEquals(List.of("request_reply.reply_destination.fenced"),
                        alerts.stream().map(CommitRetry.Alert::faultId).toList());
                // stays fenced: the same sink never takes the transactional id back, so nothing is sent again
                assertThrows(ReplyDestinationFault.Fenced.class, () -> committer(sink).commit(cycle));
            } else {
                assertTrue(alerts.isEmpty(), "recovered in place: no fence alert");
                assertNotEquals(WorkerState.Status.PAUSED, state.evaluate());
            }
        }

        if (fenced) {
            // restart: a new sink with a new producer takes the id; commits the same Cycle object (producer recovery
            // only, a real restart re-fetches the requests and re-runs the Handlers)
            try (var restarted = new KafkaReplySink(new KafkaProducer<>(producerProps), replies,
                    Map.of("main", requests), groupMeta)) {
                assertTrue(committer(restarted).commit(cycle).failures().isEmpty());
            }
        }

        var seen = new ArrayList<String>();
        var cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (var c = new KafkaConsumer<byte[], byte[]>(cp)) {
            c.assign(List.of(new TopicPartition(replies, 0)));
            long end = System.nanoTime() + Duration.ofSeconds(4).toNanos();
            while (System.nanoTime() < end) {
                c.poll(Duration.ofMillis(200)).forEach(r -> seen.add(
                        new String(r.headers().lastHeader("correlation_id").value(), StandardCharsets.UTF_8)));
            }
        }
        assertEquals(List.of("c-0", "c-1", "c-2", "c-3", "c-4"), seen.stream().sorted().toList(),
                "exactly one committed reply per request");
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            long next = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
                    .get(new TopicPartition(requests, 0)).offset();
            assertEquals(N, next, "positions advanced once, with the replies");
        }
    }
}
