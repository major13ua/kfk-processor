package xme.common.kfkprocessor.requestreply.autoconfig;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
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

/**
 * AC-01 end to end. Wiring decisions fixed by this test:
 * <ul>
 * <li>Kafka: {@code spring.kafka.bootstrap-servers}; Redis: {@code xme.request-reply.allowance-store.redis-uri}.</li>
 * <li>Wire: request headers {@code correlation_id}, {@code request_key}, {@code created_at} (ISO-8601);
 * the reply carries {@code correlation_id} and {@code request_key} headers.</li>
 * <li>Default codec with no codec beans: request payload UTF-8 text is the REQ (String), RES text is
 * its UTF-8 bytes (toString).</li>
 * <li>Consistency lag source: the {@code created_at} header, else the record timestamp.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(classes = RequestReplyEndToEndIT.App.class)
class RequestReplyEndToEndIT {

    static final String ID = UUID.randomUUID().toString().substring(0, 8);
    static final String HIGH = "req-high-" + ID;
    static final String LOW = "req-low-" + ID;
    static final String REPLIES = "rep-" + ID;

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    @Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration(RequestReplyAutoConfiguration.class)
    static class App {
        @Bean
        RequestReplyHandler<String, String, String> handler() {
            return (ctx, req) -> "pong:" + req;
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(HIGH, 1, (short) 1), new NewTopic(LOW, 1, (short) 1),
                    new NewTopic(REPLIES, 1, (short) 1))).all().get();
        }
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("xme.request-reply.allowance-store.redis-uri",
                () -> "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        r.add("xme.request-reply.worker-identity", () -> "worker-" + ID);
        r.add("xme.request-reply.group-id", () -> "grp-" + ID);
        r.add("xme.request-reply.reply-destination", () -> REPLIES);
        r.add("xme.request-reply.rate-budget-per-second", () -> "1000");
        r.add("xme.request-reply.lanes[0].name", () -> "high");
        r.add("xme.request-reply.lanes[0].source", () -> HIGH);
        r.add("xme.request-reply.lanes[0].weight", () -> "3");
        r.add("xme.request-reply.lanes[1].name", () -> "low");
        r.add("xme.request-reply.lanes[1].source", () -> LOW);
        r.add("xme.request-reply.lanes[1].weight", () -> "1");
        r.add("xme.request-reply.max-payload-bytes", () -> "1048576");
        r.add("xme.request-reply.draw-per-round", () -> "100");
        r.add("xme.request-reply.probe-interval", () -> "200ms");
    }

    @Autowired
    MeterRegistry registry;

    // AC-01
    @Test
    void requestIsAnsweredWithHandlerReplyAndSameCorrelationIdAndRequestKey() {
        Instant created = Instant.now().minusSeconds(2);
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()))) {
            var rec = new ProducerRecord<byte[], byte[]>(HIGH, null, "ping".getBytes(StandardCharsets.UTF_8));
            rec.headers().add(new RecordHeader("correlation_id", "corr-42".getBytes(StandardCharsets.UTF_8)));
            rec.headers().add(new RecordHeader("request_key", "key-7".getBytes(StandardCharsets.UTF_8)));
            rec.headers().add(new RecordHeader("created_at", created.toString().getBytes(StandardCharsets.UTF_8)));
            producer.send(rec);
            producer.flush();
        }

        ConsumerRecord<byte[], byte[]> reply = awaitReply();

        assertThat(header(reply, "correlation_id")).isEqualTo("corr-42");
        assertThat(header(reply, "request_key")).isEqualTo("key-7");
        assertThat(new String(reply.value(), StandardCharsets.UTF_8)).isEqualTo("pong:ping");
        // Consistency lag taken from created_at (about 2s), not implausible
        var lag = registry.find("requestreply.consistency.lag").tag("lane", "high").timer();
        assertThat(lag).isNotNull();
        assertThat(lag.count()).isEqualTo(1);
        assertThat(lag.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(2000);
    }

    private ConsumerRecord<byte[], byte[]> awaitReply() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (var consumer = new KafkaConsumer<byte[], byte[]>(p)) {
            consumer.assign(List.of(new TopicPartition(REPLIES, 0)));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (var rec : consumer.poll(Duration.ofMillis(200))) {
                    return rec;
                }
            }
        }
        throw new AssertionError("no reply within 30s on " + REPLIES);
    }

    private static String header(ConsumerRecord<byte[], byte[]> rec, String name) {
        Header h = rec.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
