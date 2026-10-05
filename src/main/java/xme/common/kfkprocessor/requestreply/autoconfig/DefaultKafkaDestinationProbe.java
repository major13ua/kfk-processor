package xme.common.kfkprocessor.requestreply.autoconfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthorizationException;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;

/**
 * Default probe: the broker reports whether this principal may write the reply topic and read every request lane,
 * and whether it may use its transactional id (no data is written, no transaction is started).
 */
final class DefaultKafkaDestinationProbe implements DestinationProbe {

    private static final long TIMEOUT_SECONDS = 10;

    private final Map<String, Object> clientProperties;
    private final String topic;
    private final List<String> laneSources;
    private final String transactionalId;
    private final java.util.function.Function<Map<String, Object>, Admin> adminFactory;

    DefaultKafkaDestinationProbe(Map<String, Object> clientProperties, String topic, List<String> laneSources,
            String transactionalId) {
        this(clientProperties, topic, laneSources, transactionalId, Admin::create);
    }

    /** Test seam: the Admin client factory. */
    DefaultKafkaDestinationProbe(Map<String, Object> clientProperties, String topic, List<String> laneSources,
            String transactionalId, java.util.function.Function<Map<String, Object>, Admin> adminFactory) {
        this.clientProperties = clientProperties;
        this.topic = topic;
        this.laneSources = laneSources;
        this.transactionalId = transactionalId;
        this.adminFactory = adminFactory;
    }

    @Override
    public void probe() {
        Admin admin = adminFactory.apply(clientProperties);
        try {
            List<String> topics = new ArrayList<>();
            topics.add(topic);
            topics.addAll(laneSources);
            Map<String, TopicDescription> described = admin.describeTopics(topics,
                    new DescribeTopicsOptions().includeAuthorizedOperations(true))
                    .allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            require(described.get(topic), AclOperation.WRITE, "reply destination '" + topic + "'");
            for (String source : laneSources) {
                require(described.get(source), AclOperation.READ, "request lane '" + source + "'");
            }
            checkTransactionalId(admin);
            checkNotCompacted(admin);
        } catch (ExecutionException e) {
            throw fault(e.getCause(), topic, laneSources);
        } catch (TimeoutException e) {
            throw new ReplyDestinationFault.Unavailable("reply destination unavailable", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReplyDestinationFault.Unavailable("interrupted", e);
        } finally {
            admin.close(Duration.ZERO); // the answers are in: do not wait on an unreachable cluster while closing
        }
    }

    private static void require(TopicDescription d, AclOperation op, String what) {
        if (d != null && d.authorizedOperations() != null && !d.authorizedOperations().contains(op)) {
            throw new ReplyDestinationFault.PermissionDenied(
                    "no " + op + " permission on " + what, null);
        }
    }

    /**
     * The broker answers "describe" on the transactional id only to a principal with an ACL on it (WRITE, which the
     * transactions need, implies it); a transactional id that was never used is fine. Nothing is initialised, so the
     * live producer is not fenced.
     */
    private void checkTransactionalId(Admin admin) throws InterruptedException, TimeoutException {
        try {
            admin.describeTransactions(List.of(transactionalId)).description(transactionalId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AuthorizationException) {
                throw new ReplyDestinationFault.PermissionDenied(
                        "no permission on the transactional id '" + transactionalId + "'", e.getCause());
            }
            // unknown id, or a broker that cannot describe transactions: the first commit finds out
        }
    }

    /** Reply records are keyless, a compacted topic rejects them all. Best effort: an unreadable config never refuses. */
    private void checkNotCompacted(Admin admin) throws InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config;
        try {
            config = admin.describeConfigs(List.of(resource)).values().get(resource)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            return;
        }
        ConfigEntry policy = config == null ? null : config.get("cleanup.policy");
        if (policy != null && policy.value() != null && policy.value().contains("compact")) {
            throw new ReplyDestinationFault.Invalid("reply destination '" + topic + "' is compacted (cleanup.policy="
                    + policy.value() + "), reply records are keyless and would all be rejected"
                    + " [request_reply.config.reply_destination_compacted]", null);
        }
    }

    private static ReplyDestinationFault fault(Throwable cause, String replyTopic, List<String> sources) {
        if (cause instanceof org.apache.kafka.common.errors.InvalidTopicException) {
            return new ReplyDestinationFault.Invalid("reply destination or request lane has an invalid topic name"
                    + " (reply destination '" + replyTopic + "', request lanes " + sources + ")"
                    + " [request_reply.config.reply_destination_invalid]", cause);
        }
        if (cause instanceof AuthorizationException
                || cause instanceof org.apache.kafka.common.errors.AuthenticationException) {
            return new ReplyDestinationFault.PermissionDenied("reply destination or request lane denies access", cause);
        }
        return new ReplyDestinationFault.Unavailable("reply destination unavailable", cause);
    }
}
