package xme.common.kfkprocessor.requestreply.autoconfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.AuthorizationException;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;

/** Default probe: the broker reports whether this principal may write the reply topic (no data is written). */
final class DefaultKafkaDestinationProbe implements DestinationProbe {

    private static final long TIMEOUT_SECONDS = 10;

    private final String bootstrapServers;
    private final String topic;

    DefaultKafkaDestinationProbe(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    @Override
    public void probe() {
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", bootstrapServers))) {
            TopicDescription d = admin.describeTopics(List.of(topic),
                    new DescribeTopicsOptions().includeAuthorizedOperations(true))
                    .allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).get(topic);
            if (d != null && d.authorizedOperations() != null
                    && !d.authorizedOperations().contains(AclOperation.WRITE)) {
                throw new ReplyDestinationFault.PermissionDenied("reply destination denies write access", null);
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AuthorizationException) {
                throw new ReplyDestinationFault.PermissionDenied("reply destination denies access", e.getCause());
            }
            throw new ReplyDestinationFault.Unavailable("reply destination unavailable", e.getCause());
        } catch (TimeoutException e) {
            throw new ReplyDestinationFault.Unavailable("reply destination unavailable", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReplyDestinationFault.Unavailable("interrupted", e);
        }
    }
}
