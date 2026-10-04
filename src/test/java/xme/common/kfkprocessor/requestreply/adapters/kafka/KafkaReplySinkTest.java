package xme.common.kfkprocessor.requestreply.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.CommitResult.Reason;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;

/**
 * No broker: a MockProducer seam. Header names follow the OQ-1 default (headers). Each test builds its own
 * producer and sink.
 */
class KafkaReplySinkTest {

    private static final String REPLIES = "replies";
    private static final Map<String, String> SOURCES = Map.of("high", "src-high", "low", "src-low");

    /** MockProducer whose sends can fail per record, through the callback or synchronously. */
    private static final class Producer extends MockProducer<byte[], byte[]> {
        Function<ProducerRecord<byte[], byte[]>, RuntimeException> fault = r -> null;
        boolean viaCallback = true;

        Producer() {
            super(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            RuntimeException f = fault.apply(record);
            if (f == null) {
                return super.send(record, callback);
            }
            if (!viaCallback) {
                throw f;
            }
            if (callback != null) {
                callback.onCompletion(null, f);
            }
            return CompletableFuture.failedFuture(f);
        }
    }

    private static KafkaReplySink sink(Producer p) {
        return new KafkaReplySink(p, REPLIES, SOURCES, new ConsumerGroupMetadata("grp", -1, "", java.util.Optional.empty()));
    }

    private static ReplyRecord reply(String lane, int partition, long position, String corr, byte[] value) {
        return new ReplyRecord(lane, partition, position, corr, "key-" + corr, value, false);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // AC-05
    @Test
    void transactionalIdIsStablePerIdentityAndDiffersBetweenIdentities() {
        assertEquals(KafkaReplySink.transactionalId("worker-1"), KafkaReplySink.transactionalId("worker-1"));
        assertFalse(KafkaReplySink.transactionalId("worker-1").equals(KafkaReplySink.transactionalId("worker-2")));
        assertTrue(KafkaReplySink.transactionalId("worker-1").contains("worker-1"));
    }

    // AC-05 (one commit = one transaction: replies and positions together)
    @Test
    void oneCommitPublishesRepliesAndCommitsNextPositionsInOneTransaction() {
        var p = new Producer();
        CommitResult result = sink(p).commit(List.of(
                reply("high", 0, 4, "c-1", b("r1")),
                reply("high", 0, 5, "c-2", b("r2")),
                reply("low", 1, 9, "c-3", b("r3"))));

        assertTrue(result.failures().isEmpty());
        assertTrue(p.transactionCommitted());
        assertFalse(p.transactionAborted());
        assertEquals(3, p.history().size());
        var first = p.history().get(0);
        assertEquals(REPLIES, first.topic());
        assertEquals("c-1", new String(first.headers().lastHeader("correlation_id").value(), StandardCharsets.UTF_8));
        assertEquals("key-c-1", new String(first.headers().lastHeader("request_key").value(), StandardCharsets.UTF_8));
        assertEquals("r1", new String(first.value(), StandardCharsets.UTF_8));

        assertEquals(1, p.consumerGroupOffsetsHistory().size(), "offsets must go in the same transaction");
        Map<TopicPartition, OffsetAndMetadata> offsets = p.consumerGroupOffsetsHistory().get(0).get("grp");
        assertEquals(6L, offsets.get(new TopicPartition("src-high", 0)).offset());
        assertEquals(10L, offsets.get(new TopicPartition("src-low", 1)).offset());
        assertEquals(2, offsets.size());
    }

    // AC-05 (per-reply failure is a typed result, not an abort)
    @Test
    void tooLargeReplyIsReportedPerRequestAndOthersStillCommit() {
        var p = new Producer();
        p.fault = r -> new String(r.headers().lastHeader("correlation_id").value(), StandardCharsets.UTF_8)
                .equals("big") ? new RecordTooLargeException("too large") : null;

        CommitResult result = sink(p).commit(List.of(
                reply("high", 0, 1, "ok-1", b("a")),
                reply("high", 0, 2, "big", b("huge")),
                reply("high", 0, 3, "ok-2", b("b"))));

        assertEquals(1, result.failures().size());
        var f = result.failures().get(0);
        assertEquals("big", f.correlationId());
        assertEquals(Reason.TOO_LARGE, f.reason());
        assertEquals(2L, f.position());
        assertTrue(p.transactionCommitted());
        assertFalse(p.transactionAborted());
        assertEquals(2, p.history().size());
        // positions of all three requests still commit, including the failed one
        assertEquals(4L, p.consumerGroupOffsetsHistory().get(0).get("grp").get(new TopicPartition("src-high", 0)).offset());
    }

    // AC-05 (a reply that cannot be encoded: no bytes, value null)
    @Test
    void unencodableReplyIsReportedPerRequestAndNothingIsSentForIt() {
        var p = new Producer();
        CommitResult result = sink(p).commit(List.of(
                reply("high", 0, 1, "ok", b("a")),
                reply("high", 0, 2, "bad", null)));

        assertEquals(1, result.failures().size());
        assertEquals("bad", result.failures().get(0).correlationId());
        assertEquals(Reason.UNENCODABLE, result.failures().get(0).reason());
        assertEquals(1, p.history().size());
        assertTrue(p.transactionCommitted());
    }

    // AC-05 (destination faults are distinct and abort everything)
    @Test
    void permissionDeniedIsADistinctTypedFaultAndNothingCommits() {
        var p = new Producer();
        p.fault = r -> new TopicAuthorizationException(java.util.Set.of(REPLIES));
        assertThrows(ReplyDestinationFault.PermissionDenied.class,
                () -> sink(p).commit(List.of(reply("high", 0, 1, "c", b("a")))));
        assertFalse(p.transactionCommitted());
        assertTrue(p.consumerGroupOffsetsHistory().isEmpty());
    }

    @Test
    void destinationUnavailableIsADistinctTypedFaultAndNothingCommits() {
        var p = new Producer();
        p.fault = r -> new TimeoutException("metadata not available");
        assertThrows(ReplyDestinationFault.Unavailable.class,
                () -> sink(p).commit(List.of(reply("high", 0, 1, "c", b("a")))));
        assertFalse(p.transactionCommitted());
        assertTrue(p.consumerGroupOffsetsHistory().isEmpty());
    }

    @Test
    void destinationUnavailableThrownSynchronouslyOnSendIsAlsoTyped() {
        var p = new Producer();
        p.viaCallback = false;
        p.fault = r -> new TimeoutException("Topic not present in metadata after 100 ms");
        assertThrows(ReplyDestinationFault.Unavailable.class,
                () -> sink(p).commit(List.of(reply("high", 0, 1, "c", b("a")))));
        assertFalse(p.transactionCommitted());
    }

    private static ReplyRecord withFallback(String lane, int partition, long position, String corr, byte[] value,
            byte[] fallback) {
        return new ReplyRecord(lane, partition, position, corr, "key-" + corr, value, false, fallback);
    }

    private static String corr(ProducerRecord<byte[], byte[]> r) {
        return new String(r.headers().lastHeader("correlation_id").value(), StandardCharsets.UTF_8);
    }

    // AC-08 (too large primary: the fallback goes out in the same transaction, one record for that request)
    @Test
    void tooLargeReplyIsSubstitutedByItsFallbackInTheSameTransaction() {
        var p = new Producer();
        p.fault = r -> new String(r.value(), StandardCharsets.UTF_8).equals("huge")
                ? new RecordTooLargeException("too large") : null;

        CommitResult result = sink(p).commit(List.of(
                reply("high", 0, 1, "ok-1", b("a")),
                withFallback("high", 0, 2, "big", b("huge"), b("undeliverable-big")),
                reply("high", 0, 3, "ok-2", b("b"))));

        assertEquals(1, result.failures().size());
        assertEquals("big", result.failures().get(0).correlationId());
        assertEquals(Reason.TOO_LARGE, result.failures().get(0).reason());
        assertTrue(result.failures().get(0).substituted());
        assertEquals(3, p.history().size(), "exactly one record per request");
        var big = p.history().stream().filter(r -> corr(r).equals("big")).toList();
        assertEquals(1, big.size());
        assertEquals("undeliverable-big", new String(big.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("key-big", new String(big.get(0).headers().lastHeader("request_key").value(), StandardCharsets.UTF_8));
        assertTrue(p.transactionCommitted());
        assertFalse(p.transactionAborted());
        assertEquals(1, p.consumerGroupOffsetsHistory().size());
        assertEquals(4L, p.consumerGroupOffsetsHistory().get(0).get("grp").get(new TopicPartition("src-high", 0)).offset());
    }

    // AC-08 (client-side limit, nothing sent for the primary)
    @Test
    void unencodableReplyIsSubstitutedByItsFallback() {
        var p = new Producer();
        CommitResult result = sink(p).commit(List.of(
                withFallback("high", 0, 2, "bad", null, b("undeliverable-bad"))));

        assertEquals(Reason.UNENCODABLE, result.failures().get(0).reason());
        assertTrue(result.failures().get(0).substituted());
        assertEquals(1, p.history().size());
        assertEquals("undeliverable-bad", new String(p.history().get(0).value(), StandardCharsets.UTF_8));
        assertTrue(p.transactionCommitted());
    }

    // AC-08
    @Test
    void rejectedReplyIsSubstitutedByItsFallback() {
        var p = new Producer();
        p.fault = r -> new String(r.value(), StandardCharsets.UTF_8).equals("bad")
                ? new org.apache.kafka.common.errors.CorruptRecordException("rejected") : null;
        CommitResult result = sink(p).commit(List.of(
                withFallback("high", 0, 2, "rej", b("bad"), b("undeliverable-rej"))));

        assertEquals(Reason.REJECTED, result.failures().get(0).reason());
        assertTrue(result.failures().get(0).substituted());
        assertEquals(1, p.history().size());
        assertEquals("undeliverable-rej", new String(p.history().get(0).value(), StandardCharsets.UTF_8));
    }

    // AC-08 (a reply that sends fine never triggers its fallback)
    @Test
    void fallbackIsNotSentWhenThePrimaryReplyIsDelivered() {
        var p = new Producer();
        CommitResult result = sink(p).commit(List.of(
                withFallback("high", 0, 1, "ok", b("a"), b("undeliverable-ok"))));

        assertTrue(result.failures().isEmpty());
        assertEquals(1, p.history().size());
        assertEquals("a", new String(p.history().get(0).value(), StandardCharsets.UTF_8));
    }

    // AC-08 (without a fallback the old behaviour holds: reported, not substituted)
    @Test
    void failureWithoutFallbackIsReportedAsNotSubstituted() {
        var p = new Producer();
        CommitResult result = sink(p).commit(List.of(reply("high", 0, 2, "bad", null)));

        assertFalse(result.failures().get(0).substituted());
        assertEquals(0, p.history().size());
    }
}
