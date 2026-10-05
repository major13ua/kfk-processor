package xme.common.kfkprocessor.requestreply.performance;

import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.KAFKA;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import xme.common.kfkprocessor.requestreply.failure.FailureHarness;
import xme.common.kfkprocessor.requestreply.failure.FailureHarness.Ids;
import xme.common.kfkprocessor.requestreply.failure.FailureHarness.Worker;

/** Helpers of the T17 suite on top of the T16 {@link FailureHarness}: multi-partition topics, many lanes, many workers. */
final class PerfHarness {

    private PerfHarness() {
    }

    static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    static void createTopic(String name, int partitions) {
        try (Admin admin = FailureHarness.admin()) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get(30, TimeUnit.SECONDS);
            // wait for every partition leader: producing into a topic that is still electing leaders loses batches
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (true) {
                try {
                    var d = admin.describeTopics(List.of(name)).allTopicNames().get(30, TimeUnit.SECONDS).get(name);
                    if (d.partitions().size() == partitions && d.partitions().stream().allMatch(p -> p.leader() != null)) {
                        break;
                    }
                } catch (java.util.concurrent.ExecutionException e) {
                    if (!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) {
                        throw e;
                    }
                    // topic not hosted yet: retry until the deadline
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("no leaders for " + name);
                }
                Thread.sleep(100);
            }
            awaitMetadataVisible(name, partitions);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Waits until a fresh client sees every partition of the topic with a leader (metadata propagated to brokers). */
    private static void awaitMetadataVisible(String name, int partitions) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        try (var probe = new KafkaProducer<byte[], byte[]>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000))) {
            while (true) {
                try {
                    var infos = probe.partitionsFor(name);
                    if (infos.size() == partitions && infos.stream().allMatch(i -> i.leader() != null)) {
                        return;
                    }
                } catch (org.apache.kafka.common.KafkaException e) {
                    // metadata not visible yet: retry until the deadline
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("topic metadata not visible for " + name);
                }
                Thread.sleep(100);
            }
        }
    }

    /** One group: shared group id and reply topic; the identity is per worker. */
    record Group(String group, String replies, String suffix) {
        static Group fresh() {
            String s = PerfHarness.suffix();
            createTopic("rep-" + s, 1);
            return new Group("grp-" + s, "rep-" + s, s);
        }

        String identity(int worker) {
            return "worker-" + suffix + "-" + worker;
        }
    }

    /** Lane name to request topic and weight, in order. */
    record LaneSpec(String name, String topic, double weight) {
    }

    /** Worker properties for any number of lanes. */
    static Map<String, Object> props(Group g, String identity, long budget, int draw, List<LaneSpec> lanes) {
        Map<String, Object> p = new LinkedHashMap<>(FailureHarness.workerProps(
                new Ids("unused", g.replies(), g.group(), identity), budget, draw, Duration.ofSeconds(5),
                Duration.ofSeconds(20)));
        p.remove("xme.request-reply.lanes[0].name");
        p.remove("xme.request-reply.lanes[0].source");
        p.remove("xme.request-reply.lanes[0].weight");
        for (int i = 0; i < lanes.size(); i++) {
            p.put("xme.request-reply.lanes[" + i + "].name", lanes.get(i).name());
            p.put("xme.request-reply.lanes[" + i + "].source", lanes.get(i).topic());
            p.put("xme.request-reply.lanes[" + i + "].weight", Double.toString(lanes.get(i).weight()));
        }
        return p;
    }

    /** Sum of a counter over the registries of several workers (0 when absent). */
    static double sum(List<Worker> workers, String name, String... tags) {
        double total = 0;
        for (Worker w : workers) {
            var c = w.registry.find(name).tags(tags).counter();
            total += c == null ? 0 : c.count();
        }
        return total;
    }

    static double count(Worker w, String name) {
        var c = w.registry.find(name).counter();
        return c == null ? 0 : c.count();
    }

    /** Produces {@code n} requests with the Request Key as record key (one key stays on one partition); key chosen by {@code keyOf}. */
    static void produce(String topic, String prefix, int n, IntFunction<String> keyOf) {
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.LINGER_MS_CONFIG, 20,
                ProducerConfig.BATCH_SIZE_CONFIG, 131072,
                // a loaded or re-bootstrapping broker must not fail the setup: idempotent retries within generous timeouts
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 120_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 180_000))) {
            String created = java.time.Instant.now().toString();
            java.util.concurrent.atomic.AtomicReference<Exception> failed = new java.util.concurrent.atomic.AtomicReference<>();
            for (int i = 0; i < n; i++) {
                String id = prefix + "-" + i;
                byte[] key = keyOf.apply(i).getBytes(StandardCharsets.UTF_8);
                var rec = new ProducerRecord<byte[], byte[]>(topic, null, key, ("req-" + i).getBytes(StandardCharsets.UTF_8));
                rec.headers().add(new RecordHeader("correlation_id", id.getBytes(StandardCharsets.UTF_8)));
                rec.headers().add(new RecordHeader("request_key", key));
                rec.headers().add(new RecordHeader("created_at", created.getBytes(StandardCharsets.UTF_8)));
                producer.send(rec, (m, e) -> {
                    if (e != null) {
                        failed.compareAndSet(null, e);
                    }
                });
            }
            producer.flush();
            if (failed.get() != null) {
                throw new IllegalStateException("producing the backlog to " + topic + " failed", failed.get());
            }
        }
    }

    /** Thread-safe list of Handler start times (millis). */
    static final class Starts {
        private final ConcurrentLinkedQueue<Long> times = new ConcurrentLinkedQueue<>();

        void mark() {
            times.add(System.currentTimeMillis());
        }

        int count() {
            return times.size();
        }

        List<Long> sorted() {
            List<Long> l = new ArrayList<>(times);
            java.util.Collections.sort(l);
            return l;
        }
    }

    /** Accepted per whole second since the first start, as text. */
    static String perSecond(List<Long> sorted) {
        if (sorted.isEmpty()) {
            return "[]";
        }
        long t0 = sorted.get(0);
        java.util.TreeMap<Long, Long> m = new java.util.TreeMap<>();
        sorted.forEach(t -> m.merge((t - t0) / 1000, 1L, Long::sum));
        return m.toString();
    }
}
