package xme.common.kfkprocessor.requestreply.autoconfig;

import static xme.common.kfkprocessor.TestcontainersConfiguration.newKafka;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;

/** The probe against a real broker (no ACLs: every operation is allowed, so denial is covered by the unit test). */
@Testcontainers
class DefaultKafkaDestinationProbeIT {

    @Container
    static KafkaContainer kafka = newKafka();

    @Test
    void passesOnARealBrokerForExistingTopicsAndAnUnusedTransactionalId() throws Exception {
        String id = UUID.randomUUID().toString().substring(0, 8);
        Map<String, Object> client = Map.of("bootstrap.servers", kafka.getBootstrapServers());
        try (Admin admin = Admin.create(client)) {
            admin.createTopics(List.of(new NewTopic("rep-" + id, 1, (short) 1), new NewTopic("lane-" + id, 1, (short) 1)))
                    .all().get();
        }
        var probe = new DefaultKafkaDestinationProbe(client, "rep-" + id, List.of("lane-" + id), "txn-" + id);
        assertDoesNotThrow(probe::probe);
    }

    @Test
    void aMissingLaneIsUnavailableNotPermissionDenied() {
        var probe = new DefaultKafkaDestinationProbe(Map.of("bootstrap.servers", kafka.getBootstrapServers()),
                "no-such-reply", List.of("no-such-lane"), "txn-x");
        assertInstanceOf(ReplyDestinationFault.Unavailable.class,
                assertThrows(ReplyDestinationFault.class, probe::probe));
    }

    @Test
    void aCompactedReplyTopicIsInvalid() throws Exception {
        String id = UUID.randomUUID().toString().substring(0, 8);
        Map<String, Object> client = Map.of("bootstrap.servers", kafka.getBootstrapServers());
        try (Admin admin = Admin.create(client)) {
            admin.createTopics(List.of(
                    new NewTopic("rep-" + id, 1, (short) 1).configs(Map.of("cleanup.policy", "compact")),
                    new NewTopic("lane-" + id, 1, (short) 1))).all().get();
        }
        var probe = new DefaultKafkaDestinationProbe(client, "rep-" + id, List.of("lane-" + id), "txn-" + id);
        ReplyDestinationFault e = assertThrows(ReplyDestinationFault.class, probe::probe);
        assertInstanceOf(ReplyDestinationFault.Invalid.class, e);
        org.junit.jupiter.api.Assertions.assertTrue(
                e.getMessage().contains("request_reply.config.reply_destination_compacted"), e.getMessage());
    }
}
