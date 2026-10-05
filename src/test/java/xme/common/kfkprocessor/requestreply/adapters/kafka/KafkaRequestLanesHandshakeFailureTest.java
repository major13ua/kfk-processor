package xme.common.kfkprocessor.requestreply.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.ports.LaneAccessDeniedException;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * Review r2 B6 (F16): a TLS handshake failure is not an authorization error, so today it escapes the poll as a raw
 * Kafka exception and the loop retries every 10 ms with no pause and no alert. It must surface as the typed fault
 * the loop turns into a pause and an alert.
 */
class KafkaRequestLanesHandshakeFailureTest {

    @Test
    void handshakeFailureOnAPollIsSurfacedAsAFaultTheLoopPausesOn() {
        var props = new RequestReplyProperties();
        var lane = new RequestReplyProperties.Lane();
        lane.setName("high");
        lane.setSource("lane-high");
        lane.setWeight(1);
        props.setLanes(List.of(lane));
        var consumer = new MockConsumer<byte[], byte[]>("earliest");
        var lanes = new KafkaRequestLanes(consumer, props, new WorkerMetrics() {
            public void accepted(String l, long c) { }
            public void consistencyLag(String l, java.time.Duration d, boolean i) { }
            public void state(State s) { }
            public void errorReply(ErrorCategory c) { }
            public void handlerTimeout() { }
            public void commitAttempt() { }
            public void cycleDuration(java.time.Duration d) { }
            public void groupMembershipChange() { }
        });
        consumer.setPollException(new SslAuthenticationException("PKIX path building failed"));
        assertThrows(LaneAccessDeniedException.class, lanes::awaitAvailable);
    }
}
