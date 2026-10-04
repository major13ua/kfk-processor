package xme.common.kfkprocessor.requestreply.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * Each test creates its own topics and group, so tests share only the (never destroyed) broker.
 * Header names follow the OQ-1 default (headers).
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaRequestLanesIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    private static final Duration WINDOW = Duration.ofSeconds(45);

    /** Counting metrics: only membership changes matter here. */
    private static final class Metrics {
        final AtomicInteger changes = new AtomicInteger();

        WorkerMetrics proxy() {
            return (WorkerMetrics) java.lang.reflect.Proxy.newProxyInstance(
                    WorkerMetrics.class.getClassLoader(), new Class<?>[] {WorkerMetrics.class}, (p, m, a) -> {
                        if (m.getName().equals("groupMembershipChange")) {
                            changes.incrementAndGet();
                        }
                        return null;
                    });
        }
    }

    private record Env(String group, String high, String low) {
        static Env create(int partitions) throws Exception {
            String id = UUID.randomUUID().toString().substring(0, 8);
            var env = new Env("grp-" + id, "high-" + id, "low-" + id);
            try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(env.high, partitions, (short) 1),
                        new NewTopic(env.low, partitions, (short) 1))).all().get();
            }
            return env;
        }

        RequestReplyProperties props(String identity) {
            var p = new RequestReplyProperties();
            p.setWorkerIdentity(identity);
            p.setIdentityWindow(WINDOW);
            p.setLanes(List.of(lane("high", high), lane("low", low)));
            return p;
        }

        private static RequestReplyProperties.Lane lane(String name, String source) {
            var l = new RequestReplyProperties.Lane();
            l.setName(name);
            l.setSource(source);
            l.setWeight(1);
            return l;
        }
    }

    private static KafkaRequestLanes worker(Env env, String identity, Metrics m) {
        return new KafkaRequestLanes(env.props(identity), kafka.getBootstrapServers(), env.group(), m.proxy());
    }

    private static Properties producerProps(String txId) {
        var p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        if (txId != null) {
            p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        }
        return p;
    }

    private static ProducerRecord<byte[], byte[]> rec(String topic, int partition, String corr) {
        var r = new ProducerRecord<byte[], byte[]>(topic, partition, null, new byte[] {1});
        r.headers().add(new RecordHeader("correlation_id", corr.getBytes(StandardCharsets.UTF_8)));
        r.headers().add(new RecordHeader("request_key", ("key-" + corr).getBytes(StandardCharsets.UTF_8)));
        return r;
    }

    private static void produce(String topic, int partition, String... corrs) {
        try (var producer = new KafkaProducer<byte[], byte[]>(producerProps(null))) {
            for (String c : corrs) {
                producer.send(rec(topic, partition, c));
            }
            producer.flush();
        }
    }

    private static final Map<String, Integer> BIG = Map.of("high", 100, "low", 100);

    /** Polls a worker until the predicate holds or the deadline passes; returns everything fetched. */
    private static List<IncomingRequest> drain(KafkaRequestLanes w, Map<String, Integer> quota, int want, Duration max) {
        var all = new ArrayList<IncomingRequest>();
        long end = System.nanoTime() + max.toNanos();
        while (all.size() < want && System.nanoTime() < end) {
            all.addAll(w.fetch(quota));
        }
        return all;
    }

    // AC-14
    @Test
    void restartWithSameIdentityWithinWindowCausesNoRebalanceForTheOtherWorker() throws Exception {
        var env = Env.create(2);
        var m1 = new Metrics();
        var m2 = new Metrics();
        var w2 = worker(env, "worker-2-" + env.group(), m2);
        var w1 = worker(env, "worker-1-" + env.group(), m1);
        try {
            // Both join and settle: poll both until the group is stable (no change for 3 s).
            int stable = -1;
            long stableSince = System.nanoTime();
            long end = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < end && System.nanoTime() - stableSince < Duration.ofSeconds(3).toNanos()) {
                w1.fetch(BIG);
                w2.fetch(BIG);
                int now = m1.changes.get() + m2.changes.get();
                if (now != stable) {
                    stable = now;
                    stableSince = System.nanoTime();
                }
            }
            int before = m2.changes.get();

            // Worker 1 stops (no leave: static member) and returns with the same identity within the window.
            w1.close();
            var restartedMetrics = new Metrics();
            var w1b = worker(env, "worker-1-" + env.group(), restartedMetrics);
            try {
                long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (System.nanoTime() < until) {
                    w1b.fetch(BIG);
                    w2.fetch(BIG);
                }
                assertEquals(before, m2.changes.get(), "worker 2 saw lane reassignment during the restart");

                // Both keep consuming: each gets data from its own lane partition.
                produce(env.high(), 0, "a-0", "a-1");
                produce(env.high(), 1, "b-0", "b-1");
                var got = new ArrayList<IncomingRequest>();
                long until2 = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (got.size() < 4 && System.nanoTime() < until2) {
                    got.addAll(w1b.fetch(BIG));
                    got.addAll(w2.fetch(BIG));
                }
                assertEquals(4, got.size());
            } finally {
                w1b.close();
            }
        } finally {
            w2.close();
        }
    }

    // AC-14 (read committed only)
    @Test
    void fetchSeesOnlyCommittedRecords() throws Exception {
        var env = Env.create(1);
        try (var tx = new KafkaProducer<byte[], byte[]>(producerProps("tx-" + env.group()))) {
            tx.initTransactions();
            tx.beginTransaction();
            tx.send(rec(env.high(), 0, "aborted-1"));
            tx.flush();
            tx.abortTransaction();
            tx.beginTransaction();
            tx.send(rec(env.high(), 0, "committed-1"));
            tx.commitTransaction();
        }
        try (var w = worker(env, "worker-" + env.group(), new Metrics())) {
            var got = drain(w, BIG, 1, Duration.ofSeconds(30));
            // allow any further (wrongly visible) records to arrive
            got.addAll(drain(w, BIG, Integer.MAX_VALUE, Duration.ofSeconds(3)));
            assertEquals(List.of("committed-1"), got.stream().map(IncomingRequest::correlationId).toList());
        }
    }

    // AC-14 (positions advance only through the Cycle transaction)
    @Test
    void fetchNeverCommitsOffsets() throws Exception {
        var env = Env.create(1);
        produce(env.high(), 0, "x-0", "x-1", "x-2");
        try (var w = worker(env, "worker-" + env.group(), new Metrics())) {
            assertEquals(3, drain(w, BIG, 3, Duration.ofSeconds(30)).size());
            drain(w, BIG, Integer.MAX_VALUE, Duration.ofSeconds(7)); // longer than the default auto-commit interval
        } // close() must not commit either
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets(env.group()).partitionsToOffsetAndMetadata().get();
            assertTrue(offsets.isEmpty(), "offsets were committed: " + offsets);
        }
    }

    // AC-14 (fetch up to the quota, no loss)
    @Test
    void fetchRespectsQuotaAndRepeatedFetchesDeliverEverythingOnceInOrder() throws Exception {
        var env = Env.create(1);
        var corrs = new String[10];
        for (int i = 0; i < corrs.length; i++) {
            corrs[i] = "q-" + i;
        }
        produce(env.high(), 0, corrs);
        var quota = Map.of("high", 3, "low", 3);
        try (var w = worker(env, "worker-" + env.group(), new Metrics())) {
            var all = new ArrayList<String>();
            long end = System.nanoTime() + Duration.ofSeconds(40).toNanos();
            while (all.size() < corrs.length && System.nanoTime() < end) {
                var batch = w.fetch(quota);
                assertTrue(batch.size() <= 3, "quota exceeded: " + batch.size());
                batch.forEach(r -> all.add(r.correlationId()));
            }
            assertEquals(List.of(corrs), all);
            assertFalse(all.isEmpty());
        }
    }
}
