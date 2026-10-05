package xme.common.kfkprocessor.requestreply.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;

/** Pure parts of the Kafka lanes adapter, no broker. Header names are the OQ-1 default (headers). */
class KafkaRequestLanesTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ConsumerRecord<byte[], byte[]> record(String corr, String key) {
        var r = new ConsumerRecord<byte[], byte[]>("lane-high", 2, 17L, null, b("payload"));
        if (corr != null) {
            r.headers().add(new RecordHeader("correlation_id", b(corr)));
        }
        if (key != null) {
            r.headers().add(new RecordHeader("request_key", b(key)));
        }
        return r;
    }

    private static IncomingRequest req(String lane, long pos) {
        return new IncomingRequest(lane, 0, pos, "k", "c" + pos, Map.of(), new byte[0]);
    }

    // AC-14 (record surfaced, not thrown)
    @Test
    void wellFormedRecordIsMappedWithLanePartitionAndPosition() {
        IncomingRequest in = KafkaRequestLanes.toIncoming("high", record("c-1", "k-1"));
        assertEquals("high", in.lane());
        assertEquals(2, in.partition());
        assertEquals(17L, in.position());
        assertEquals("c-1", in.correlationId());
        assertEquals("k-1", String.valueOf(in.requestKey()));
        assertArrayEquals(b("payload"), in.payload());
    }

    @Test
    void missingCorrelationIdIsSurfacedAsMalformedNotThrown() {
        IncomingRequest in = KafkaRequestLanes.toIncoming("high", record(null, "k-1"));
        assertNotNull(in);
        assertNull(in.correlationId());
        assertEquals(17L, in.position());
        assertEquals("high", in.lane());
    }

    @Test
    void missingRequestKeyIsSurfacedAsMalformedNotThrown() {
        IncomingRequest in = KafkaRequestLanes.toIncoming("high", record("c-1", null));
        assertNotNull(in);
        assertNull(in.requestKey());
        assertEquals("c-1", in.correlationId());
    }

    @Test
    void recordMissingBothIsSurfacedAsMalformedNotThrown() {
        IncomingRequest in = KafkaRequestLanes.toIncoming("high", record(null, null));
        assertNotNull(in);
        assertNull(in.correlationId());
        assertNull(in.requestKey());
    }

    @Test
    void quotaCapsEachLaneAndPreservesOrder() {
        var polled = List.of(req("high", 0), req("low", 0), req("high", 1), req("high", 2), req("low", 1));
        var out = KafkaRequestLanes.limitByQuota(polled, Map.of("high", 2, "low", 1));
        assertEquals(List.of(req("high", 0), req("low", 0), req("high", 1)).stream().map(IncomingRequest::correlationId).toList(),
                out.stream().map(IncomingRequest::correlationId).toList());
    }

    @Test
    void laneWithoutQuotaOrZeroQuotaYieldsNothing() {
        var polled = List.of(req("high", 0), req("low", 0), req("mid", 0));
        var out = KafkaRequestLanes.limitByQuota(polled, Map.of("high", 1, "mid", 0));
        assertEquals(List.of("high"), out.stream().map(IncomingRequest::lane).toList());
    }

    // AC-09 (review B10): security settings reach the consumer
    @Test
    void consumerGetsTheSharedClientPropertiesButKeepsItsOwnSettings() {
        var props = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties();
        props.setWorkerIdentity("worker-1");
        var out = KafkaRequestLanes.consumerProperties(props,
                Map.of("bootstrap.servers", "b:9092", "security.protocol", "SASL_SSL",
                        "isolation.level", "read_uncommitted"), "grp");
        assertEquals("SASL_SSL", out.get("security.protocol"));
        assertEquals("b:9092", out.get("bootstrap.servers"));
        assertEquals("read_committed", out.get("isolation.level"));
    }

    // AC-09 (review B10): lane-read denial is a typed fault, not a raw Kafka exception
    @Test
    void readDenialOnAPollIsSurfacedAsLaneAccessDenied() {
        var props = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties();
        var lane = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties.Lane();
        lane.setName("high");
        lane.setSource("lane-high");
        lane.setWeight(1);
        props.setLanes(List.of(lane));
        var consumer = new org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]>("earliest");
        var lanes = new KafkaRequestLanes(consumer, props, new xme.common.kfkprocessor.requestreply.ports.WorkerMetrics() {
            public void accepted(String l, long c) { }
            public void consistencyLag(String l, java.time.Duration d, boolean i) { }
            public void state(State s) { }
            public void errorReply(xme.common.kfkprocessor.requestreply.api.ErrorCategory c) { }
            public void handlerTimeout() { }
            public void commitAttempt() { }
            public void cycleDuration(java.time.Duration d) { }
            public void groupMembershipChange() { }
        });
        consumer.setPollException(new org.apache.kafka.common.errors.TopicAuthorizationException(java.util.Set.of("lane-high")));
        org.junit.jupiter.api.Assertions.assertThrows(
                xme.common.kfkprocessor.requestreply.ports.LaneAccessDeniedException.class, lanes::awaitAvailable);
        consumer.setPollException(new org.apache.kafka.common.errors.GroupAuthorizationException("grp"));
        org.junit.jupiter.api.Assertions.assertThrows(
                xme.common.kfkprocessor.requestreply.ports.LaneAccessDeniedException.class, lanes::keepAlive);
    }
}
