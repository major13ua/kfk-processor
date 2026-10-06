package xme.common.kfkprocessor;

import java.util.function.Consumer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Single source of the container images and factories used by the tests; no other class names an image. */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    public static final String KAFKA_IMAGE = "apache/kafka:4.2.2";
    public static final String REDIS_IMAGE = "redis:7";
    public static final int REDIS_PORT = 6379;

    public static KafkaContainer newKafka() {
        return new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE));
    }

    public static KafkaContainer newKafka(Consumer<KafkaContainer> customizer) {
        KafkaContainer kafka = newKafka();
        customizer.accept(kafka);
        return kafka;
    }

    /** Redis with its port exposed; the caller starts it (or lets {@code @Container} do so). */
    public static GenericContainer<?> newRedis() {
        return new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE)).withExposedPorts(REDIS_PORT);
    }

    /**
     * The one broker of the whole test JVM: started on first use, never stopped by a test class or a Spring context
     * (the Ryuk reaper removes it at JVM exit). Tests isolate themselves with unique topic and group names.
     */
    public static KafkaContainer sharedKafka() {
        return SharedKafka.INSTANCE;
    }

    @Bean(destroyMethod = "")
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return sharedKafka();
    }

    private static final class SharedKafka {
        static final KafkaContainer INSTANCE = newKafka();

        static {
            INSTANCE.start();
        }
    }

}
