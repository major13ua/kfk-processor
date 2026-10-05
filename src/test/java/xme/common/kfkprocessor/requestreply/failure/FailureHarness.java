package xme.common.kfkprocessor.requestreply.failure;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.autoconfig.RequestReplyAutoConfiguration;

/** Shared harness of the T16 failure-scenario suite: real Kafka + Redis, per-test topics, committed-only reader. */
public final class FailureHarness {

    /** Provisional NFR numbers (spec.md section 6): referenced by name so a changed NFR is changed in one place. */
    public static final Duration STOP_WITHIN = Duration.ofSeconds(5);
    public static final Duration RESUME_WITHIN = Duration.ofSeconds(30);
    public static final double BUDGET_TOLERANCE = 1.10;
    public static final Duration WAIT = Duration.ofSeconds(60);

    public static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");
    public static final GenericContainer<?> REDIS;
    public static final int REDIS_PORT = freePort();

    static {
        KAFKA.start();
        // fixed host port so the store can be stopped and started again on the same address
        REDIS = new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(REDIS_PORT), new ExposedPort(6379))));
        REDIS.start();
    }

    private FailureHarness() {
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String redisUri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS_PORT;
    }

    /** Fresh names per test: requests topic, replies topic, group, identity. Topics are created. */
    public record Ids(String requests, String replies, String group, String identity) {
        public static Ids fresh() {
            String id = UUID.randomUUID().toString().substring(0, 8);
            Ids ids = new Ids("req-" + id, "rep-" + id, "grp-" + id, "worker-" + id);
            try (Admin admin = admin()) {
                admin.createTopics(List.of(new NewTopic(ids.requests, 1, (short) 1),
                        new NewTopic(ids.replies, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
                awaitTopicReady(admin, ids.requests);
                awaitTopicReady(admin, ids.replies);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            return ids;
        }
    }

    /** Waits until the topic's single partition has a leader and a fresh producer sees it (metadata propagated). */
    static void awaitTopicReady(Admin admin, String topic) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        try (var probe = new KafkaProducer<byte[], byte[]>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000))) {
            while (true) {
                try {
                    var d = admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
                    boolean led = !d.partitions().isEmpty() && d.partitions().stream().allMatch(p -> p.leader() != null);
                    if (led) {
                        var infos = probe.partitionsFor(topic);
                        if (infos.size() == d.partitions().size() && infos.stream().allMatch(i -> i.leader() != null)) {
                            return;
                        }
                    }
                } catch (java.util.concurrent.ExecutionException | org.apache.kafka.common.KafkaException e) {
                    // not visible yet: retry until the deadline
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("topic not ready: " + topic);
                }
                Thread.sleep(100);
            }
        }
    }

    public static Admin admin() {
        return Admin.create(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    /** Worker configuration: one lane, short probe interval, handler timeout inside the Cycle deadline. */
    public static Map<String, Object> workerProps(Ids ids, long budgetPerSecond, int drawPerRound, Duration handlerTimeout,
            Duration commitWindow) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
        p.put("xme.request-reply.allowance-store.redis-uri", redisUri());
        p.put("xme.request-reply.worker-identity", ids.identity());
        p.put("xme.request-reply.group-id", ids.group());
        p.put("xme.request-reply.reply-destination", ids.replies());
        p.put("xme.request-reply.rate-budget-per-second", Long.toString(budgetPerSecond));
        p.put("xme.request-reply.lanes[0].name", "main");
        p.put("xme.request-reply.lanes[0].source", ids.requests());
        p.put("xme.request-reply.lanes[0].weight", "1");
        p.put("xme.request-reply.draw-per-round", Integer.toString(drawPerRound));
        p.put("xme.request-reply.probe-interval", "200ms");
        p.put("xme.request-reply.handler-timeout", handlerTimeout.toMillis() + "ms");
        p.put("xme.request-reply.commit-window", commitWindow.toMillis() + "ms");
        p.put("xme.request-reply.identity-window", "10s");
        return p;
    }

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration(RequestReplyAutoConfiguration.class)
    public static class Base {
    }

    /** A running worker (in-process Spring context). */
    public static final class Worker implements AutoCloseable {
        public final ConfigurableApplicationContext ctx;
        public final MeterRegistry registry;

        Worker(ConfigurableApplicationContext ctx, MeterRegistry registry) {
            this.ctx = ctx;
            this.registry = registry;
        }

        /** Published state gauge: 0 running, 1 paused, 2 stalled (WorkerMetrics.State ordinal). */
        public double state() {
            return registry.get("requestreply.state").gauge().value();
        }

        double errorReplies(String category) {
            var c = registry.find("requestreply.errorreply").tag("category", category).counter();
            return c == null ? 0 : c.count();
        }

        @Override
        public void close() {
            ctx.close();
        }
    }

    public static Worker startWorker(Map<String, Object> props, RequestReplyHandler<String, String, String> handler,
            Consumer<GenericApplicationContext> extraBeans) {
        MeterRegistry registry = new SimpleMeterRegistry();
        ApplicationContextInitializer<ConfigurableApplicationContext> init = ctx -> {
            GenericApplicationContext g = (GenericApplicationContext) ctx;
            g.registerBean("handler", RequestReplyHandler.class, () -> handler);
            g.registerBean("meterRegistry", MeterRegistry.class, () -> registry);
            extraBeans.accept(g);
        };
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Base.class)
                .web(WebApplicationType.NONE).bannerMode(Banner.Mode.OFF).logStartupInfo(false)
                .properties(props).initializers(init)
                // a command-line argument outranks the host application.properties, which turns the starter off
                .run("--xme.request-reply.enabled=true");
        return new Worker(ctx, registry);
    }

    public static Worker startWorker(Map<String, Object> props, RequestReplyHandler<String, String, String> handler) {
        return startWorker(props, handler, g -> {
        });
    }

    /** Sends one request per correlation id (request key = correlation id). */
    public static void produce(String topic, List<String> correlationIds) {
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                // setup must not fail on a slow broker: idempotent retries within generous timeouts
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 120_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 180_000))) {
            java.util.concurrent.atomic.AtomicReference<Exception> failed = new java.util.concurrent.atomic.AtomicReference<>();
            for (String id : correlationIds) {
                var rec = new ProducerRecord<byte[], byte[]>(topic, null, ("req-" + id).getBytes(StandardCharsets.UTF_8));
                rec.headers().add(new RecordHeader("correlation_id", id.getBytes(StandardCharsets.UTF_8)));
                rec.headers().add(new RecordHeader("request_key", id.getBytes(StandardCharsets.UTF_8)));
                rec.headers().add(new RecordHeader("created_at", java.time.Instant.now().toString().getBytes(StandardCharsets.UTF_8)));
                producer.send(rec, (m, e) -> {
                    if (e != null) {
                        failed.compareAndSet(null, e);
                    }
                });
            }
            producer.flush();
            if (failed.get() != null) {
                throw new IllegalStateException("producing to " + topic + " failed", failed.get());
            }
        }
    }

    public static List<String> correlationIds(String prefix, int n) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ids.add(prefix + "-" + i);
        }
        return ids;
    }

    /** {@code type} is the wire header telling a reply ({@code reply}) from an Error Reply ({@code error_reply}). */
    public record Rep(String correlationId, String requestKey, String body, String type) {
        public Rep(String correlationId, String requestKey, String body) {
            this(correlationId, requestKey, body, null);
        }

        public boolean isErrorReply() {
            return "error_reply".equals(type);
        }
    }

    /** All replies visible to a committed-only reader right now (polls for {@code listen}). */
    public static List<Rep> readCommitted(String topic, Duration listen) {
        return readCommitted(topic, listen, Integer.MAX_VALUE);
    }

    /** Committed-only read; returns early once {@code stopAt} replies were seen. */
    public static List<Rep> readCommitted(String topic, Duration listen, int stopAt) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        List<Rep> out = new ArrayList<>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(p)) {
            consumer.assign(List.of(new TopicPartition(topic, 0)));
            long deadline = System.nanoTime() + listen.toNanos();
            while (System.nanoTime() < deadline && out.size() < stopAt) {
                for (var rec : consumer.poll(Duration.ofMillis(200))) {
                    out.add(new Rep(header(rec.headers().lastHeader("correlation_id")),
                            header(rec.headers().lastHeader("request_key")),
                            rec.value() == null ? null : new String(rec.value(), StandardCharsets.UTF_8),
                            header(rec.headers().lastHeader("type"))));
                }
            }
        }
        return out;
    }

    /**
     * Waits (bounded) until {@code expected} replies are committed, then listens {@code settle} longer so extra
     * (duplicate) replies would show up. Returns everything seen.
     */
    public static List<Rep> awaitReplies(String topic, int expected, Duration timeout, Duration settle) {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<Rep> seen = List.of();
        while (System.nanoTime() < deadline) {
            seen = readCommitted(topic, Duration.ofSeconds(2), expected);
            if (seen.size() >= expected) {
                return readCommitted(topic, settle);
            }
        }
        throw new AssertionError("expected " + expected + " committed replies on " + topic + " within " + timeout
                + " but saw " + seen.size());
    }

    private static String header(Header h) {
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    public static void await(String what, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted waiting for " + what);
            }
        }
        throw new AssertionError("timed out after " + timeout + " waiting for: " + what);
    }

    public static void pause(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** True while the worker's static member (group.instance.id = identity) is in the consumer group. */
    public static boolean inGroup(String group, String identity) {
        try (Admin admin = admin()) {
            var d = admin.describeConsumerGroups(List.of(group)).describedGroups().get(group).get(10, TimeUnit.SECONDS);
            return d.members().stream().anyMatch(m -> m.groupInstanceId().map(identity::equals).orElse(false));
        } catch (Exception e) {
            return false;
        }
    }

    /** Records every Handler invocation: correlation id, idempotency key, wall-clock millis. */
    public static final class HandlerLog {
        public record Call(String correlationId, String idempotencyKey, long atMillis) {
        }

        public final List<Call> calls = new CopyOnWriteArrayList<>();

        public void record(String correlationId, String idempotencyKey) {
            calls.add(new Call(correlationId, idempotencyKey, System.currentTimeMillis()));
        }

        public int count() {
            return calls.size();
        }

        public Map<String, Integer> perCorrelation() {
            Map<String, Integer> m = new LinkedHashMap<>();
            calls.forEach(c -> m.merge(c.correlationId(), 1, Integer::sum));
            return m;
        }

        public List<Long> times() {
            List<Long> t = new ArrayList<>();
            calls.forEach(c -> t.add(c.atMillis()));
            Collections.sort(t);
            return t;
        }
    }

    /** Largest number of timestamps inside any sliding 1 s window. */
    public static int maxInAnySecond(List<Long> sortedMillis) {
        int max = 0;
        int lo = 0;
        for (int hi = 0; hi < sortedMillis.size(); hi++) {
            while (sortedMillis.get(hi) - sortedMillis.get(lo) >= 1000) {
                lo++;
            }
            max = Math.max(max, hi - lo + 1);
        }
        return max;
    }

    /** Reconciliation of requests against replies: every request answered exactly once, nothing extra. */
    public static void assertReconciled(List<String> requested, List<Rep> replies) {
        Map<String, Integer> byCorrelation = new LinkedHashMap<>();
        replies.forEach(r -> byCorrelation.merge(r.correlationId(), 1, Integer::sum));
        List<String> lost = requested.stream().filter(id -> !byCorrelation.containsKey(id)).toList();
        List<String> duplicated = byCorrelation.entrySet().stream().filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey).toList();
        List<String> unknown = byCorrelation.keySet().stream().filter(id -> !requested.contains(id)).toList();
        org.assertj.core.api.Assertions.assertThat(lost).as("lost replies").isEmpty();
        org.assertj.core.api.Assertions.assertThat(duplicated).as("duplicated committed replies").isEmpty();
        org.assertj.core.api.Assertions.assertThat(unknown).as("replies for unknown requests").isEmpty();
    }
}
