package xme.common.kfkprocessor.requestreply.autoconfig;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.engine.CycleLoop;
import xme.common.kfkprocessor.requestreply.engine.WorkerState;
import xme.common.kfkprocessor.requestreply.ports.AllowanceStore;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * T15 wiring, no containers. {@code xme.request-reply.auto-start=false} keeps the loop from starting;
 * user-supplied port beans (RequestLanes, ReplySink, AllowanceStore, DestinationProbe) replace the Kafka/Redis
 * defaults (back-off), so no broker is needed.
 */
class RequestReplyAutoConfigurationTest {

    static final RequestLanes LANES = new RequestLanes() {
        public List<IncomingRequest> fetch(Map<String, Integer> q) { return List.of(); }
        public void pause() { }
        public void resume() { }
    };
    static final ReplySink SINK = new ReplySink() {
        public CommitResult commit(List<ReplyRecord> replies) { return new CommitResult(List.of()); }
    };
    static final AllowanceStore STORE = new AllowanceStore() {
        public long reserve(long units) { return units; }
        public void giveBack(long units) { }
    };
    static final DestinationProbe PROBE = () -> { };

    @Configuration(proxyBeanMethods = false)
    static class HandlerConfig {
        @Bean
        RequestReplyHandler<String, String, String> handler() {
            return (ctx, req) -> req;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SecondHandlerConfig {
        @Bean
        RequestReplyHandler<String, String, String> second() {
            return (ctx, req) -> req;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class NonStringHandlerConfig {
        @Bean
        RequestReplyHandler<Long, Integer, Integer> handler() {
            return (ctx, req) -> req;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PortsConfig {
        @Bean RequestLanes lanes() { return LANES; }
        @Bean ReplySink sink() { return SINK; }
        @Bean AllowanceStore store() { return STORE; }
        @Bean DestinationProbe probe() { return PROBE; }
    }

    private ApplicationContextRunner runner(String... extra) {
        String[] noStart = new String[extra.length + 1];
        System.arraycopy(extra, 0, noStart, 0, extra.length);
        noStart[extra.length] = "xme.request-reply.auto-start=false";
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RequestReplyAutoConfiguration.class))
                .withPropertyValues(RequestReplyTestProps.valid(noStart));
    }

    // AC-01 (registration)
    @Test
    void isRegisteredInTheAutoConfigurationImportsFile() throws IOException {
        var found = Collections.list(getClass().getClassLoader()
                .getResources("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports"));
        String all = found.stream().map(RequestReplyAutoConfigurationTest::read).reduce("", String::concat);
        assertThat(all).contains(RequestReplyAutoConfiguration.class.getName());
    }

    private static String read(URL url) {
        try (var in = url.openStream()) {
            return new String(in.readAllBytes());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // AC-01, edge case "adopting service has no web starter"
    @Test
    void loadsWithoutAnyWebOrRestClientClass() {
        runner().withUserConfiguration(HandlerConfig.class, PortsConfig.class)
                .withClassLoader(new FilteredClassLoader(
                        "org.springframework.web", "jakarta.servlet", "org.springframework.http.client",
                        "org.springframework.boot.restclient", "org.springframework.boot.webmvc"))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx).hasSingleBean(CycleLoop.class);
                    assertThat(ctx).hasSingleBean(WorkerState.class);
                    assertThat(ctx).hasSingleBean(RequestReplyProperties.class);
                    assertThat(ctx).hasSingleBean(WorkerMetrics.class);
                });
    }

    // AC-02 via T3 StartupValidator: no Handler bean
    @Test
    void noHandlerBeanRefusesStartWithTheT3Message() {
        runner().withUserConfiguration(PortsConfig.class).run(ctx -> {
            assertThat(ctx).hasFailed();
            Throwable root = rootCause(ctx.getStartupFailure());
            assertThat(root).isInstanceOf(ConfigurationRefusedException.class);
            assertThat(((ConfigurationRefusedException) root).code())
                    .isEqualTo("request_reply.config.handler_missing");
            assertThat(root.getMessage()).contains("Exactly one Handler is required");
        });
    }

    @Test
    void twoHandlerBeansRefuseStart() {
        runner().withUserConfiguration(HandlerConfig.class, SecondHandlerConfig.class, PortsConfig.class)
                .run(ctx -> {
                    assertThat(ctx).hasFailed();
                    assertThat(rootCause(ctx.getStartupFailure()))
                            .isInstanceOfSatisfying(ConfigurationRefusedException.class,
                                    e -> assertThat(e.code()).isEqualTo("request_reply.config.handler_missing"));
                });
    }

    // B14: Handler generic types other than String are refused at startup
    @Test
    void handlerWithNonStringTypesRefusesStart() {
        runner().withUserConfiguration(NonStringHandlerConfig.class, PortsConfig.class).run(ctx -> {
            assertThat(ctx).hasFailed();
            assertThat(rootCause(ctx.getStartupFailure()))
                    .isInstanceOfSatisfying(ConfigurationRefusedException.class,
                            e -> assertThat(e.code()).isEqualTo("request_reply.config.handler_types_unsupported"));
        });
    }

    // B10: spring.kafka security settings reach every Kafka client of the worker
    @Test
    void kafkaSecuritySettingsAreCollectedForTheWorkersClients() {
        var env = new org.springframework.mock.env.MockEnvironment()
                .withProperty("spring.kafka.bootstrap-servers", "b1:9093,b2:9093")
                .withProperty("spring.kafka.security.protocol", "SASL_SSL")
                .withProperty("spring.kafka.properties.sasl.mechanism", "PLAIN");
        Map<String, Object> out = RequestReplyAutoConfiguration.clientProperties(env);
        assertThat(out.get("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(out.get("sasl.mechanism")).isEqualTo("PLAIN");
        assertThat(String.valueOf(out.get("bootstrap.servers"))).contains("b1:9093").contains("b2:9093");
        assertThat(out).doesNotContainKey("client.id");
    }

    // AC-15 via T3: identity is explicit
    @Test
    void missingWorkerIdentityRefusesStart() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RequestReplyAutoConfiguration.class))
                .withPropertyValues("xme.request-reply.reply-destination=replies",
                        "xme.request-reply.rate-budget-per-second=1000",
                        "xme.request-reply.lanes[0].name=high",
                        "xme.request-reply.lanes[0].source=requests-high",
                        "xme.request-reply.lanes[0].weight=1",
                        "xme.request-reply.auto-start=false")
                .withUserConfiguration(HandlerConfig.class, PortsConfig.class)
                .run(ctx -> {
                    assertThat(ctx).hasFailed();
                    assertThat(rootCause(ctx.getStartupFailure()))
                            .isInstanceOfSatisfying(ConfigurationRefusedException.class,
                                    e -> assertThat(e.code()).isEqualTo("request_reply.config.identity_missing"));
                });
    }

    @Test
    void disabledByPropertyRegistersNothingAndDoesNotRequireAHandler() {
        runner("xme.request-reply.enabled=false").run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx).doesNotHaveBean(CycleLoop.class);
            assertThat(ctx).doesNotHaveBean(RequestReplyProperties.class);
        });
    }

    // Back-off: a user bean for a port wins over the default adapter.
    @Test
    void userSuppliedPortBeansBackOffTheDefaultAdapters() {
        runner().withUserConfiguration(HandlerConfig.class, PortsConfig.class).run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx.getBean(RequestLanes.class)).isSameAs(LANES);
            assertThat(ctx.getBean(ReplySink.class)).isSameAs(SINK);
            assertThat(ctx.getBean(AllowanceStore.class)).isSameAs(STORE);
            assertThat(ctx.getBean(DestinationProbe.class)).isSameAs(PROBE);
        });
    }

    // public-api.md section 3 names the property min-lane-share (T3 currently binds min-lane-share-percent)
    @Test
    void minLaneShareBindsUnderTheContractName() {
        runner("xme.request-reply.min-lane-share=10").withUserConfiguration(HandlerConfig.class, PortsConfig.class)
                .run(ctx -> assertThat(ctx.getBean(RequestReplyProperties.class).getMinLaneSharePercent())
                        .isEqualTo(10));
    }

    @Test
    void cycleLoopIsNotStartedWhenAutoStartIsFalse() {
        runner().withUserConfiguration(HandlerConfig.class, PortsConfig.class).run(ctx -> {
            Thread.sleep(200);
            assertThat(Thread.getAllStackTraces().keySet())
                    .noneMatch(t -> "request-reply-cycle-loop".equals(t.getName()) && t.isAlive());
        });
    }

    private static Throwable rootCause(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }
}
