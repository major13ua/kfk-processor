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
}
