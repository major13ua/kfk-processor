package xme.common.kfkprocessor.requestreply.autoconfig;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.ports.AllowanceStore;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;

/**
 * Review r2 B6 (F16): a {@code KafkaConnectionDetails} bean and an SSL bundle ({@code spring.kafka.ssl.bundle}) must
 * reach the Kafka clients of the worker, as Spring Boot's KafkaAutoConfiguration does for its own clients. The
 * consumer, producer and Admin probe share one client-properties map, read here from the probe the
 * auto-configuration builds.
 */
class RequestReplySslPropagationTest {

    private static final String CERT = """
            -----BEGIN CERTIFICATE-----
            MIIDATCCAemgAwIBAgIUXOowgJUYCMD6OQ6eUe7fr4WLkGowDQYJKoZIhvcNAQEL
            BQAwDzENMAsGA1UEAwwEdGVzdDAgFw0yNjEwMDUxNDMxMjdaGA8yMTI2MDkxMTE0
            MzEyN1owDzENMAsGA1UEAwwEdGVzdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
            AQoCggEBAK/OBoC/WHXDHcIafg33nPIf7K4wffgpMPY35MIX/j8hIcIQ9OKuqBRz
            4JZttCVSGisXQZeUrXyzBjdG4Roqd7OHD+X/ftGS8XDE3jbcR0q5w/caP/6JjgC8
            vmARmtL5fLH7jVV8zqJxVqHzAcnlgDSZk6ctG7XvGZgGdCKk+T7HpIMMpArJEBHI
            w9y51q9E4/6WXsEBGA2v9FtVf8ft2727NRK51llPtfdmE6hj9/h3ZY7QZjxtngL/
            Jyr5XB8fLUHUjGfYYoNPyGJiuUP/20tEJBtrQRGzTOI2cqECIy0CdEttBb7lsZ8X
            7fUsLrhvccEk8W5/GdL3pvviN2P+88UCAwEAAaNTMFEwHQYDVR0OBBYEFB9Dl6Yq
            8ubv32xj2/xqSzB6x1GqMB8GA1UdIwQYMBaAFB9Dl6Yq8ubv32xj2/xqSzB6x1Gq
            MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBABFnfdPUmZU0nlHv
            UksSzwmR6M8oPa3KR5gG7m9nO1yfwccG2nMBGE8pc+Ba7hkEGWXwJV3Q5S5P/lMg
            MAUdskDZ//CN2AON+sqs8ljZY06H9uPZIKpU37AO+FYKRHoVMolCa6TA/V1ea36I
            XgKXMqA1Cfn/CtrQUvfMiRgX7rXn7tBn+wdPk4gThqqUA7PVhSSl5wuJ4XbYlWwJ
            mUojTYachuzvpzky+sjwpXsijCoJihS+F9RqNTkjulMv7Qojvq7NgJwNGHGmHTpt
            qYajC4K5dB38VnlwRN0BGxGY7AhqZd5F7abA9coZ8JmvslgGe4ej17R7e3u9zoct
            4W3oEBA=
            -----END CERTIFICATE-----
            """;

    @Configuration(proxyBeanMethods = false)
    static class Beans {
        @Bean
        RequestReplyHandler<String, String, String> handler() {
            return (ctx, req) -> req;
        }
        @Bean
        RequestLanes lanes() {
            return new RequestLanes() {
                public List<IncomingRequest> fetch(Map<String, Integer> q) { return List.of(); }
                public void pause() { }
                public void resume() { }
            };
        }
        @Bean
        ReplySink sink() {
            return replies -> new CommitResult(List.of());
        }
        @Bean
        AllowanceStore store() {
            return new AllowanceStore() {
                public long reserve(long units) { return units; }
                public void giveBack(long units) { }
            };
        }
        @Bean
        KafkaConnectionDetails kafkaConnectionDetails() {
            return () -> List.of("details-host:9093");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> probeClientProperties(Object probe) throws ReflectiveOperationException {
        Field f = DefaultKafkaDestinationProbe.class.getDeclaredField("clientProperties");
        f.setAccessible(true);
        return (Map<String, Object>) f.get(probe);
    }

    @Test
    void connectionDetailsAndSslBundleReachTheKafkaClients() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(SslAutoConfiguration.class,
                        RequestReplyAutoConfiguration.class))
                .withPropertyValues(RequestReplyTestProps.valid("xme.request-reply.auto-start=false"))
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=property-host:9092",
                        "spring.kafka.ssl.bundle=kafka",
                        "spring.ssl.bundle.pem.kafka.truststore.certificate=" + CERT)
                .withUserConfiguration(Beans.class)
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    Map<String, Object> props = probeClientProperties(
                            ctx.getBean(xme.common.kfkprocessor.requestreply.ports.DestinationProbe.class));
                    // the connection-details bootstrap servers win over spring.kafka.bootstrap-servers
                    assertThat(String.valueOf(props.get("bootstrap.servers"))).contains("details-host:9093")
                            .doesNotContain("property-host");
                    // the bundle is applied the way Boot does: security.protocol and the bundle-backed SSL engine
                    assertThat(props.get("security.protocol")).isEqualTo("SSL");
                    assertThat(String.valueOf(props.get("ssl.engine.factory.class")))
                            .contains("SslBundleSslEngineFactory");
                    assertThat(props.get(org.springframework.boot.ssl.SslBundle.class.getName())).isNotNull();
                });
    }
}
