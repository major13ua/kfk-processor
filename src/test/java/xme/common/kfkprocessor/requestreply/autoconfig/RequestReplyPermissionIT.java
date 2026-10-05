package xme.common.kfkprocessor.requestreply.autoconfig;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.engine.CommitRetry;
import xme.common.kfkprocessor.requestreply.engine.WorkerState;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;

/**
 * AC-09 at start. Seams (no broker ACLs needed):
 * <ul>
 * <li>a user {@link DestinationProbe} bean replaces the default probe; the auto-configuration calls it at start
 * and every {@code xme.request-reply.probe-interval} while paused;</li>
 * <li>a user {@code Consumer<CommitRetry.Alert>} bean receives the configuration fault (default: error log);</li>
 * <li>a {@link WorkerState} bean and the {@code requestreply.state} gauge (RUNNING 0, PAUSED 1) expose the state.</li>
 * </ul>
 */
@Testcontainers
@SpringBootTest(classes = RequestReplyPermissionIT.App.class)
class RequestReplyPermissionIT {

    static final String ID = UUID.randomUUID().toString().substring(0, 8);
    static final String REQUESTS = "req-" + ID;
    static final String REPLIES = "rep-" + ID;
    static final String GROUP = "grp-" + ID;
    static final String IDENTITY = "worker-" + ID;

    static final AtomicBoolean DENIED = new AtomicBoolean(true);
    static final AtomicInteger HANDLED = new AtomicInteger();
    static final List<CommitRetry.Alert> ALERTS = new CopyOnWriteArrayList<>();

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration(RequestReplyAutoConfiguration.class)
    static class App {
        @Bean
        RequestReplyHandler<String, String, String> handler() {
            return (ctx, req) -> {
                HANDLED.incrementAndGet();
                return "pong:" + req;
            };
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        DestinationProbe destinationProbe() {
            return () -> {
                if (DENIED.get()) {
                    throw new ReplyDestinationFault.PermissionDenied("denied by test", null);
                }
            };
        }

        @Bean
        Consumer<CommitRetry.Alert> requestReplyAlertListener() {
            return ALERTS::add;
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(REQUESTS, 2, (short) 1), new NewTopic(REPLIES, 1, (short) 1)))
                    .all().get();
        }
        r.add("xme.request-reply.enabled", () -> "true"); // the host application.properties turns the starter off
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("xme.request-reply.allowance-store.redis-uri",
                () -> "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        r.add("xme.request-reply.worker-identity", () -> IDENTITY);
        r.add("xme.request-reply.group-id", () -> GROUP);
        r.add("xme.request-reply.reply-destination", () -> REPLIES);
        r.add("xme.request-reply.rate-budget-per-second", () -> "1000");
        r.add("xme.request-reply.lanes[0].name", () -> "high");
        r.add("xme.request-reply.lanes[0].source", () -> REQUESTS);
        r.add("xme.request-reply.lanes[0].weight", () -> "1");
        r.add("xme.request-reply.probe-interval", () -> "200ms");
    }

    @Autowired
    WorkerState state;

    @Autowired
    MeterRegistry registry;

    // AC-09
    @Test
    void permissionDeniedAtStartPausesStaysInGroupAndResumesOnRestoreWithoutLosingTheRequest() throws Exception {
        // paused(permission) and a configuration fault reported at start
        await(() -> state.pauseReason() == WorkerState.PauseReason.PERMISSION, "paused(permission)");
        assertThat(state.evaluate()).isEqualTo(WorkerState.Status.PAUSED);
        assertThat(registry.get("requestreply.state").gauge().value()).isEqualTo(1.0);
        assertThat(ALERTS).anySatisfy(a -> {
            assertThat(a.reason()).isEqualTo(WorkerState.PauseReason.PERMISSION);
            assertThat(a.faultId()).isEqualTo("request_reply.reply_destination.permission_denied");
        });

        // stays in its group with the same lanes (the join completes asynchronously after the pause is first seen)
        await(() -> {
            try {
                return assignmentOf(IDENTITY).size() == 2;
            } catch (Exception e) {
                return false;
            }
        }, "group assignment of both partitions");
        Set<TopicPartition> before = assignmentOf(IDENTITY);
        assertThat(before).hasSize(2);

        // a request sent while denied is neither handled nor committed nor answered
        send("corr-1", "key-1", "ping");
        Thread.sleep(2000);
        assertThat(HANDLED.get()).isZero();
        assertThat(repliesCount()).isZero();
        assertThat(committedOffsets()).isZero();
        assertThat(assignmentOf(IDENTITY)).isEqualTo(before);

        // permission restored: resumes by itself, handles the request exactly once, same assignment
        DENIED.set(false);
        await(() -> repliesCount() == 1, "reply after restore");
        assertThat(HANDLED.get()).isEqualTo(1);
        assertThat(state.pauseReason()).isNull();
        assertThat(registry.get("requestreply.state").gauge().value()).isEqualTo(0.0);
        assertThat(assignmentOf(IDENTITY)).isEqualTo(before);
    }

    private void send(String corr, String key, String body) {
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()))) {
            var rec = new ProducerRecord<byte[], byte[]>(REQUESTS, 0, null, body.getBytes(StandardCharsets.UTF_8));
            rec.headers().add(new RecordHeader("correlation_id", corr.getBytes(StandardCharsets.UTF_8)));
            rec.headers().add(new RecordHeader("request_key", key.getBytes(StandardCharsets.UTF_8)));
            producer.send(rec);
            producer.flush();
        }
    }

    private int repliesCount() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (var consumer = new KafkaConsumer<byte[], byte[]>(p)) {
            TopicPartition tp = new TopicPartition(REPLIES, 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            int n = 0;
            long deadline = System.nanoTime() + Duration.ofMillis(800).toNanos();
            while (System.nanoTime() < deadline) {
                n += consumer.poll(Duration.ofMillis(200)).count();
            }
            return n;
        }
    }

    private Set<TopicPartition> assignmentOf(String instanceId) throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            var group = admin.describeConsumerGroups(List.of(GROUP)).all().get().get(GROUP);
            return group.members().stream()
                    .filter(m -> m.groupInstanceId().map(instanceId::equals).orElse(false))
                    .flatMap(m -> m.assignment().topicPartitions().stream())
                    .collect(java.util.stream.Collectors.toSet());
        }
    }

    private long committedOffsets() throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            return admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get().values().stream()
                    .mapToLong(o -> o.offset()).sum();
        }
    }

    private static void await(Supplier<Boolean> condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.get()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("timed out waiting for " + what);
    }
}
