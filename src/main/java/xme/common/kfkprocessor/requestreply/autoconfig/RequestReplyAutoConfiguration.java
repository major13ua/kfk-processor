package xme.common.kfkprocessor.requestreply.autoconfig;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.Environment;
import xme.common.kfkprocessor.requestreply.adapters.allowance.Bucket4jBudgetCounters;
import xme.common.kfkprocessor.requestreply.adapters.allowance.RedisAllowanceStore;
import xme.common.kfkprocessor.requestreply.adapters.kafka.KafkaReplySink;
import xme.common.kfkprocessor.requestreply.adapters.kafka.KafkaRequestLanes;
import xme.common.kfkprocessor.requestreply.adapters.metrics.MicrometerWorkerMetrics;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.engine.CommitRetry;
import xme.common.kfkprocessor.requestreply.engine.CycleCommitter;
import xme.common.kfkprocessor.requestreply.engine.CycleLoop;
import xme.common.kfkprocessor.requestreply.engine.HandlerExecutor;
import xme.common.kfkprocessor.requestreply.engine.Intake;
import xme.common.kfkprocessor.requestreply.engine.WorkerState;
import xme.common.kfkprocessor.requestreply.ports.AllowanceStore;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * Builds a request-reply worker from one Handler bean and {@code xme.request-reply.*}. Ports (RequestLanes,
 * ReplySink, AllowanceStore, DestinationProbe) back off when the application supplies its own bean. No web or REST
 * client type is used here.
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "xme.request-reply", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RequestReplyProperties.class)
public class RequestReplyAutoConfiguration {

    static final String CHECK = "requestReplyConfigurationCheck";
    private static final System.Logger LOG = System.getLogger(RequestReplyAutoConfiguration.class.getName());
    /** Consistency lags beyond this are counted as implausible instead of recorded. */
    private static final Duration MAX_PLAUSIBLE_LAG = Duration.ofDays(1);

    /** Marker bean: configuration validated (T3) before any adapter is built. */
    record ConfigurationCheck() {
    }

    @Bean(CHECK)
    static ConfigurationCheck requestReplyConfigurationCheck(RequestReplyProperties props,
            ConfigurableListableBeanFactory beanFactory) {
        String[] handlerNames = beanFactory.getBeanNamesForType(RequestReplyHandler.class);
        StartupValidator validator = new StartupValidator(msg -> LOG.log(System.Logger.Level.INFO, msg));
        validator.validate(props, handlerNames.length);
        ResolvableType handlerType = beanFactory.getMergedBeanDefinition(handlerNames[0]).getResolvableType()
                .as(RequestReplyHandler.class);
        validator.validateHandlerTypes(handlerType.getGeneric(0).resolve(), handlerType.getGeneric(1).resolve(),
                handlerType.getGeneric(2).resolve());
        return new ConfigurationCheck();
    }

    @Bean
    @ConditionalOnMissingBean
    WorkerMetrics requestReplyWorkerMetrics(ObjectProvider<MeterRegistry> registry) {
        return new MicrometerWorkerMetrics(registry.getIfAvailable(SimpleMeterRegistry::new));
    }

    @Bean
    @ConditionalOnMissingBean
    WorkerState requestReplyWorkerState(RequestReplyProperties props, WorkerMetrics metrics) {
        return new WorkerState(Clock.systemUTC(), props.getStallThreshold(), metrics);
    }

    @Bean
    @DependsOn(CHECK)
    @ConditionalOnMissingBean
    RequestLanes requestReplyLanes(RequestReplyProperties props, Environment env, WorkerMetrics metrics) {
        return new KafkaRequestLanes(props, clientProperties(env), props.getGroupId(), metrics);
    }

    @Bean
    @DependsOn(CHECK)
    @ConditionalOnMissingBean
    ReplySink requestReplySink(RequestReplyProperties props, Environment env, RequestLanes lanes) {
        // offsets travel with the lanes consumer's live group metadata, so the group fences a stale worker
        java.util.function.Supplier<org.apache.kafka.clients.consumer.ConsumerGroupMetadata> metadata =
                lanes instanceof KafkaRequestLanes kafkaLanes ? kafkaLanes::groupMetadata : null;
        return new KafkaReplySink(props, clientProperties(env), props.getGroupId(), metadata);
    }

    @Bean
    @DependsOn(CHECK)
    @ConditionalOnMissingBean
    AllowanceStore requestReplyAllowanceStore(RequestReplyProperties props) {
        String uri = props.getAllowanceStore().getRedisUri();
        if (uri == null || uri.isBlank()) {
            throw new ConfigurationRefusedException("request_reply.config.allowance_store_missing",
                    "No allowance store: set xme.request-reply.allowance-store.redis-uri or provide an AllowanceStore bean."
                            + " [request_reply.config.allowance_store_missing]");
        }
        Bucket4jBudgetCounters counters = Bucket4jBudgetCounters.connect(uri,
                "xme:request-reply:" + props.getGroupId() + ":budget", props.getRateBudgetPerSecond());
        return new ClosingAllowanceStore(counters);
    }

    @Bean
    @DependsOn(CHECK)
    @ConditionalOnMissingBean
    DestinationProbe requestReplyDestinationProbe(RequestReplyProperties props, Environment env) {
        return new DefaultKafkaDestinationProbe(clientProperties(env), props.getReplyDestination(),
                props.getLanes().stream().map(RequestReplyProperties.Lane::getSource).toList(),
                KafkaReplySink.transactionalId(props.getGroupId(), props.getWorkerIdentity()));
    }

    @Bean
    @ConditionalOnMissingBean(name = "requestReplyAlertListener")
    Consumer<CommitRetry.Alert> requestReplyAlertListener() {
        return a -> LOG.log(System.Logger.Level.ERROR,
                "Request-reply worker paused (" + a.reason() + "): " + a.faultId());
    }

    @Bean
    @DependsOn(CHECK)
    CycleLoop<?, ?, ?> requestReplyCycleLoop(RequestReplyProperties props, RequestReplyHandler<?, ?, ?> handler,
            RequestLanes lanes, ReplySink sink, AllowanceStore store, DestinationProbe probe, WorkerState state,
            WorkerMetrics metrics, @Qualifier("requestReplyAlertListener") Consumer<CommitRetry.Alert> alert) {
        return assemble(props, handler, lanes, sink, store, probe, state, metrics, alert);
    }

    @Bean
    RequestReplyLifecycle requestReplyLifecycle(CycleLoop<?, ?, ?> loop, RequestLanes lanes, DestinationProbe probe,
            WorkerState state, @Qualifier("requestReplyAlertListener") Consumer<CommitRetry.Alert> alert,
            RequestReplyProperties props) {
        return new RequestReplyLifecycle(loop, lanes, probe, state, alert, props.getProbeInterval(),
                props.isAutoStart());
    }

    private static <K, REQ, RES> CycleLoop<K, REQ, RES> assemble(RequestReplyProperties props,
            RequestReplyHandler<?, ?, ?> rawHandler, RequestLanes lanes, ReplySink sink, AllowanceStore store,
            DestinationProbe probe, WorkerState state, WorkerMetrics metrics, Consumer<CommitRetry.Alert> alert) {
        @SuppressWarnings("unchecked")
        RequestReplyHandler<K, REQ, RES> handler = (RequestReplyHandler<K, REQ, RES>) rawHandler;
        Clock clock = Clock.systemUTC();
        Map<String, Double> weights = new LinkedHashMap<>();
        props.getLanes().forEach(l -> weights.put(l.getName(), l.getWeight()));
        Intake intake = new Intake(store, lanes, state, clock, weights, props.getMinLaneSharePercent() / 100.0,
                props.getMaxPayloadBytes(), props.getDrawPerRound(), props.getProbeInterval(), alert);
        @SuppressWarnings("unchecked")
        Function<IncomingRequest, REQ> decoder =
                r -> (REQ) new String(r.payload(), StandardCharsets.UTF_8);
        HandlerExecutor<K, REQ, RES> executor = new HandlerExecutor<>(handler, decoder, props.getHandlerTimeout());
        CycleCommitter<K, RES> committer = new CycleCommitter<>(sink,
                reply -> String.valueOf(reply.data()).getBytes(StandardCharsets.UTF_8),
                RequestReplyAutoConfiguration::encodeError);
        CommitRetry<K, RES> retry = new CommitRetry<>(committer, probe, state, clock, props.getProbeInterval(),
                props.getCommitRetryAttempts(), alert, metrics);
        return new CycleLoop<>(intake, executor, retry, state, metrics, clock, props.effectiveCycleDeadline(),
                RequestReplyAutoConfiguration::createdAt, MAX_PLAUSIBLE_LAG, props.getProbeInterval(), alert);
    }

    /** created_at header (ISO-8601), else the record timestamp, else now. */
    static Instant createdAt(IncomingRequest r) {
        byte[] h = r.headers().get("created_at");
        if (h != null) {
            try {
                return Instant.parse(new String(h, StandardCharsets.UTF_8));
            } catch (RuntimeException ignored) {
                // fall through to the record timestamp
            }
        }
        byte[] ts = r.headers().get(KafkaRequestLanes.RECORD_TIMESTAMP);
        if (ts != null) {
            try {
                return Instant.ofEpochMilli(Long.parseLong(new String(ts, StandardCharsets.UTF_8)));
            } catch (RuntimeException ignored) {
                // fall through
            }
        }
        return Instant.now();
    }

    static <K> byte[] encodeError(ErrorReply<K> e) {
        String json = "{\"correlation_id\":\"" + escape(e.correlationId()) + "\",\"request_key\":\""
                + escape(String.valueOf(e.requestKey())) + "\",\"category\":\""
                + e.category().name().toLowerCase(java.util.Locale.ROOT) + "\"}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    /** JSON string escaping: quote, backslash and every control character below U+0020. */
    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    /**
     * Bootstrap servers and security settings ({@code spring.kafka.*}) shared by the consumer, producer and admin
     * client of the worker, so a secured cluster works for all three. {@code client.id} is left to each client.
     */
    static Map<String, Object> clientProperties(Environment env) {
        KafkaProperties kafka = Binder.get(env).bind("spring.kafka", KafkaProperties.class)
                .orElseGet(KafkaProperties::new);
        Map<String, Object> out = new LinkedHashMap<>(kafka.buildAdminProperties());
        out.remove("client.id");
        out.values().removeIf(java.util.Objects::isNull);
        return out;
    }

    /** Releases the Redis connection on shutdown. */
    static final class ClosingAllowanceStore extends RedisAllowanceStore implements AutoCloseable {
        private final Bucket4jBudgetCounters counters;

        ClosingAllowanceStore(Bucket4jBudgetCounters counters) {
            super(counters.counter());
            this.counters = counters;
        }

        @Override
        public void close() {
            counters.close();
        }
    }
}
