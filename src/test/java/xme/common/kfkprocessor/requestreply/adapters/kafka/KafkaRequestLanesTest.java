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

    // review r2 A1 (F12): cooperative rebalancing keeps retained partitions across a rebalance
    @Test
    void consumerUsesTheCooperativeStickyAssignor() {
        var props = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties();
        props.setWorkerIdentity("worker-1");
        var out = KafkaRequestLanes.consumerProperties(props, Map.of("bootstrap.servers", "b:9092"), "grp");
        assertEquals(org.apache.kafka.clients.consumer.CooperativeStickyAssignor.class.getName(),
                String.valueOf(out.get("partition.assignment.strategy")));
    }

    // review r2 A1 (F12): requests handed out before a rebalance that revoked their partition are stale; handing
    // them back afterwards must not make them served again (the committed offset moved on, or another member owns them)
    @Test
    void releasedRequestsOfAPartitionRevokedInBetweenAreDropped() {
        var props = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties();
        var lane = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties.Lane();
        lane.setName("high");
        lane.setSource("lane-high");
        lane.setWeight(1);
        props.setLanes(List.of(lane));
        var consumer = new org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]>("earliest");
        var tp = new org.apache.kafka.common.TopicPartition("lane-high", 0);
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
        consumer.rebalance(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        consumer.addRecord(new ConsumerRecord<>("lane-high", 0, 0L, null, b("p0")));
        consumer.addRecord(new ConsumerRecord<>("lane-high", 0, 1L, null, b("p1")));
        assertEquals(true, lanes.awaitAvailable());
        List<IncomingRequest> handedOut = lanes.fetch(Map.of("high", 2));
        assertEquals(2, handedOut.size());

        // eager: revoked, then assigned again (MockConsumer.rebalance only calls back for the difference, so the
        // eager revoke-all / assign-all is two calls)
        consumer.rebalance(List.of());
        consumer.rebalance(List.of(tp));

        lanes.release(handedOut);
        assertEquals(0, lanes.bufferedCount(), "requests of a revoked partition must not be served again");
    }

    private static KafkaRequestLanes lanesOver(org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]> consumer) {
        var props = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties();
        var lane = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties.Lane();
        lane.setName("high");
        lane.setSource("lane-high");
        lane.setWeight(1);
        props.setLanes(List.of(lane));
        return new KafkaRequestLanes(consumer, props, (xme.common.kfkprocessor.requestreply.ports.WorkerMetrics)
                java.lang.reflect.Proxy.newProxyInstance(KafkaRequestLanesTest.class.getClassLoader(),
                        new Class<?>[] {xme.common.kfkprocessor.requestreply.ports.WorkerMetrics.class},
                        (p, m, a) -> null));
    }

    private static void fill(org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]> consumer, int partition,
            int records) {
        for (int i = 0; i < records; i++) {
            consumer.addRecord(new ConsumerRecord<>("lane-high", partition, i, null, b("p" + partition + ":" + i)));
        }
    }

    // review r2 A1 (F12): a cooperative rebalance revoking another partition leaves retained requests valid
    @Test
    void requestsOfARetainedPartitionStayValidAcrossACooperativeRebalance() {
        var consumer = new org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]>("earliest");
        var p0 = new org.apache.kafka.common.TopicPartition("lane-high", 0);
        var p1 = new org.apache.kafka.common.TopicPartition("lane-high", 1);
        var lanes = lanesOver(consumer);
        consumer.rebalance(List.of(p0, p1));
        consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
        fill(consumer, 0, 2);
        lanes.awaitAvailable();
        List<IncomingRequest> handedOut = lanes.fetch(Map.of("high", 2));
        assertEquals(2, handedOut.size());

        consumer.rebalance(List.of(p0)); // cooperative: only p1 revoked

        assertEquals(false, lanes.revokedSinceFetch(handedOut.get(0)));
        lanes.release(handedOut);
        assertEquals(2, lanes.bufferedCount(), "requests of a retained partition are served again");
    }

    // review r2 A1 (F12): staleness is per fetch, so a request re-read after the reassignment is valid again
    @Test
    void aRequestFetchedBeforeARevocationIsStaleAndItsReReadIsNot() {
        var consumer = new org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]>("earliest");
        var tp = new org.apache.kafka.common.TopicPartition("lane-high", 0);
        var lanes = lanesOver(consumer);
        consumer.rebalance(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        fill(consumer, 0, 1);
        lanes.awaitAvailable();
        IncomingRequest before = lanes.fetch(Map.of("high", 1)).get(0);

        consumer.rebalance(List.of());
        consumer.rebalance(List.of(tp));
        fill(consumer, 0, 1);
        lanes.awaitAvailable();
        IncomingRequest reRead = lanes.fetch(Map.of("high", 1)).get(0);

        assertEquals(true, lanes.revokedSinceFetch(before));
        assertEquals(false, lanes.revokedSinceFetch(reRead));
    }

    // review r2 A1 (F12): after a held Cycle commits, a retained partition reads on after it, never the same requests
    @Test
    void heldCommitMovesARetainedPartitionPastTheCommittedRequests() {
        var consumer = new org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]>("earliest");
        var tp = new org.apache.kafka.common.TopicPartition("lane-high", 0);
        var lanes = lanesOver(consumer);
        consumer.rebalance(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        fill(consumer, 0, 2);
        lanes.awaitAvailable();
        List<IncomingRequest> held = lanes.fetch(Map.of("high", 2));
        consumer.seek(tp, 0L); // the read position went back to the old committed offset

        lanes.committedAfterHold(held);

        assertEquals(2L, consumer.position(tp));
    }

    @Test
    void heldCommitNeverMovesAReadPositionBackward() {
        var consumer = new org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]>("earliest");
        var tp = new org.apache.kafka.common.TopicPartition("lane-high", 0);
        var lanes = lanesOver(consumer);
        consumer.rebalance(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        fill(consumer, 0, 5);
        lanes.awaitAvailable();
        List<IncomingRequest> held = lanes.fetch(Map.of("high", 2)); // 2..4 stay buffered, position 5

        lanes.committedAfterHold(held);

        assertEquals(5L, consumer.position(tp));
        assertEquals(3, lanes.bufferedCount());
    }

    // Weight Accuracy: a lane's quota is taken from each of its partitions, not drained from the first one
    @Test
    void quotaOfALaneIsTakenFromAllItsPartitionsKeepingEachPartitionContiguousAndInOrder() {
        var consumer = new org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]>("earliest");
        var p0 = new org.apache.kafka.common.TopicPartition("lane-high", 0);
        var p1 = new org.apache.kafka.common.TopicPartition("lane-high", 1);
        var lanes = lanesOver(consumer);
        consumer.rebalance(List.of(p0, p1));
        consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
        fill(consumer, 0, 4);
        fill(consumer, 1, 4);
        lanes.awaitAvailable();

        List<IncomingRequest> out = lanes.fetch(Map.of("high", 4));

        assertEquals(4, out.size());
        assertEquals(List.of(0L, 1L), out.stream().filter(r -> r.partition() == 0).map(IncomingRequest::position).toList());
        assertEquals(List.of(0L, 1L), out.stream().filter(r -> r.partition() == 1).map(IncomingRequest::position).toList());
        assertEquals(4, lanes.bufferedCount(), "the surplus of both partitions stays buffered");
    }

    // Weight Accuracy: a partition whose own share is buffered is paused, a sibling partition of the lane is not
    @Test
    void secondPartitionOfALaneStaysUnpausedWhenTheFirstAloneFilledTheQuota() {
        var consumer = new org.apache.kafka.clients.consumer.MockConsumer<byte[], byte[]>("earliest");
        var p0 = new org.apache.kafka.common.TopicPartition("lane-high", 0);
        var p1 = new org.apache.kafka.common.TopicPartition("lane-high", 1);
        var lanes = lanesOver(consumer);
        consumer.rebalance(List.of(p0, p1));
        consumer.updateBeginningOffsets(Map.of(p0, 0L, p1, 0L));
        fill(consumer, 0, 2);
        lanes.awaitAvailable(); // buffers p0's 2 records, p1 had none yet

        lanes.fetch(Map.of("high", 2));

        assertEquals(java.util.Set.of(p0), consumer.paused());
    }
}
