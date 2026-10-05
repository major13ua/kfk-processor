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
import xme.common.kfkprocessor.requestreply.ports.GroupMembershipChanged;
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

    private static KafkaReplySink sink(MockProducer<byte[], byte[]> p) {
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
        assertEquals(KafkaReplySink.transactionalId("grp", "worker-1"), KafkaReplySink.transactionalId("grp", "worker-1"));
        assertFalse(KafkaReplySink.transactionalId("grp", "worker-1").equals(KafkaReplySink.transactionalId("grp", "worker-2")));
        assertTrue(KafkaReplySink.transactionalId("grp", "worker-1").contains("worker-1"));
    }

    // AC-05 (review A2: the same identity in another group must not share, and so fence, a transactional id)
    @Test
    void transactionalIdDiffersBetweenGroupsForTheSameIdentity() {
        assertFalse(KafkaReplySink.transactionalId("grp-a", "worker-1").equals(KafkaReplySink.transactionalId("grp-b", "worker-1")));
        assertTrue(KafkaReplySink.transactionalId("grp-a", "worker-1").contains("grp-a"));
        // no two (group, identity) pairs may concatenate to the same id
        assertFalse(KafkaReplySink.transactionalId("a-b", "c").equals(KafkaReplySink.transactionalId("a", "b-c")));
    }

    // AC-05 (review A2: offsets go in with the live consumer's generation, so a zombie is fenced by the group)
    @Test
    void offsetsAreSentWithTheCurrentGroupMetadataOfTheConsumer() {
        var captured = new java.util.ArrayList<ConsumerGroupMetadata>();
        var p = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                    ConsumerGroupMetadata metadata) {
                captured.add(metadata);
                super.sendOffsetsToTransaction(offsets, metadata);
            }
        };
        var generation = new java.util.concurrent.atomic.AtomicInteger(7);
        var sink = new KafkaReplySink(p, REPLIES, SOURCES,
                () -> new ConsumerGroupMetadata("grp", generation.get(), "member-1", java.util.Optional.of("worker-1")));

        sink.commit(List.of(reply("high", 0, 1, "c-1", b("a"))));
        generation.set(8);
        sink.commit(List.of(reply("high", 0, 2, "c-2", b("b"))));

        assertEquals(List.of(7, 8), captured.stream().map(ConsumerGroupMetadata::generationId).toList());
        assertEquals("member-1", captured.get(0).memberId());
        assertEquals(java.util.Optional.of("worker-1"), captured.get(0).groupInstanceId());
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

    // review r3 B1 / AC-08b, AC-09: an invalid reply-destination name is a destination fault, never a per-reply REJECTED
    @Test
    void invalidTopicIsADestinationFaultNotAPerReplyRejectionAndNothingCommits() {
        var p = new Producer();
        p.fault = r -> new org.apache.kafka.common.errors.InvalidTopicException("bad topic name");
        assertThrows(ReplyDestinationFault.class,
                () -> sink(p).commit(List.of(withFallback("high", 0, 1, "c", b("a"), b("undeliverable-c")))));
        assertFalse(p.transactionCommitted());
        assertTrue(p.consumerGroupOffsetsHistory().isEmpty());
    }

    @Test
    void invalidTopicThrownSynchronouslyOnSendIsAlsoADestinationFault() {
        var p = new Producer();
        p.viaCallback = false;
        p.fault = r -> new org.apache.kafka.common.errors.InvalidTopicException("bad topic name");
        assertThrows(ReplyDestinationFault.class,
                () -> sink(p).commit(List.of(reply("high", 0, 1, "c", b("a")))));
        assertFalse(p.transactionCommitted());
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

    /**
     * Models the real client: once a record of the transaction is rejected, the records sent after it in the same
     * transaction fail with the same exception (batch-mates, undrained batches); beginTransaction resets that.
     */
    private static final class CollateralProducer extends MockProducer<byte[], byte[]> {
        private RuntimeException poison;

        CollateralProducer() {
            super(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public synchronized void beginTransaction() {
            poison = null;
            super.beginTransaction();
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            if (poison == null && new String(record.value(), StandardCharsets.UTF_8).equals("huge")) {
                poison = new RecordTooLargeException("too large");
            }
            if (poison == null) {
                return super.send(record, callback);
            }
            if (callback != null) {
                callback.onCompletion(null, poison);
            }
            return CompletableFuture.failedFuture(poison);
        }
    }

    // AC-08 (review A4: one rejected reply must not demote the replies that failed only as collateral)
    @Test
    void onlyTheRejectedReplyBecomesAnErrorReplyWhenLaterSendsFailWithTheSameException() {
        var p = new CollateralProducer();
        var replies = new java.util.ArrayList<ReplyRecord>();
        replies.add(withFallback("high", 0, 0, "big", b("huge"), b("undeliverable-big")));
        for (int i = 1; i <= 50; i++) {
            replies.add(withFallback("high", 0, i, "ok-" + i, b("v" + i), b("undeliverable-ok-" + i)));
        }
        CommitResult result = sink(p).commit(replies);

        assertEquals(List.of("big"), result.failures().stream().map(CommitResult.ReplyFailure::correlationId).toList(),
                "only the rejected reply is a failure, the collateral ones are re-sent unchanged");
        assertEquals(51, p.history().size(), "exactly one record per request");
        for (var r : p.history()) {
            String c = corr(r);
            String expected = c.equals("big") ? "undeliverable-big" : "v" + c.substring(3);
            assertEquals(expected, new String(r.value(), StandardCharsets.UTF_8), "value of " + c);
        }
        assertTrue(p.transactionCommitted());
        assertEquals(51L, p.consumerGroupOffsetsHistory().get(0).get("grp").get(new TopicPartition("src-high", 0)).offset());
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

    // AC-09 (review B10): a missing WRITE on the transactional id is a permission fault, not "unavailable"
    @Test
    void transactionalIdAuthorizationFailureAtInitIsPermissionDenied() {
        var p = new Producer();
        p.initTransactionException = new org.apache.kafka.common.errors.TransactionalIdAuthorizationException("tx");
        assertThrows(ReplyDestinationFault.PermissionDenied.class,
                () -> sink(p).commit(List.of(reply("high", 0, 1, "c-1", b("a")))));
    }

    @Test
    void transactionalIdAuthorizationFailureAtCommitIsPermissionDenied() {
        var p = new Producer();
        p.commitTransactionException = new org.apache.kafka.common.errors.TransactionalIdAuthorizationException("tx");
        assertThrows(ReplyDestinationFault.PermissionDenied.class,
                () -> sink(p).commit(List.of(reply("high", 0, 1, "c-1", b("a")))));
    }

    // review r3 B3 / AC-08b, AC-17: a rebalance-rejected commit is typed "membership changed", not destination, not untyped
    private void assertMembershipChanged(RuntimeException kafka, boolean atOffsets) {
        var p = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                    ConsumerGroupMetadata metadata) {
                if (atOffsets) {
                    throw kafka;
                }
                super.sendOffsetsToTransaction(offsets, metadata);
            }
        };
        if (!atOffsets) {
            p.commitTransactionException = kafka;
        }
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> sink(p).commit(List.of(reply("high", 0, 1, "c-1", b("a")))));
        assertTrue(thrown instanceof GroupMembershipChanged, "was " + thrown);
        assertFalse(thrown instanceof ReplyDestinationFault);
        assertEquals(kafka, thrown.getCause());
        assertFalse(p.transactionCommitted());
    }

    @Test
    void commitFailedExceptionAtOffsetsIsMembershipChanged() {
        assertMembershipChanged(new org.apache.kafka.clients.consumer.CommitFailedException(), true);
    }

    @Test
    void fencedInstanceIdAtOffsetsIsMembershipChanged() {
        assertMembershipChanged(new org.apache.kafka.common.errors.FencedInstanceIdException("static member fenced"), true);
    }

    @Test
    void commitFailedExceptionAtCommitIsMembershipChanged() {
        assertMembershipChanged(new org.apache.kafka.clients.consumer.CommitFailedException(), false);
    }

    // AC-09 (review B10): security settings reach the producer
    @Test
    void producerGetsTheSharedClientProperties() {
        var props = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties();
        props.setWorkerIdentity("worker-1");
        var out = KafkaReplySink.producerProperties(props,
                Map.of("bootstrap.servers", "b:9092", "security.protocol", "SASL_SSL"), "grp");
        assertEquals("SASL_SSL", out.get("security.protocol"));
        assertEquals("b:9092", out.get("bootstrap.servers"));
        assertEquals(KafkaReplySink.transactionalId("grp", "worker-1"), out.get("transactional.id"));
    }

    // review r2 D: the client-side size pre-check uses the configured max.request.size, not the client default
    @Test
    void sizePreCheckLimitIsTheConfiguredMaxRequestSize() {
        var props = new xme.common.kfkprocessor.requestreply.api.RequestReplyProperties();
        props.setWorkerIdentity("worker-1");
        assertEquals(2000, KafkaReplySink.maxRecordBytes(KafkaReplySink.producerProperties(props,
                Map.of("bootstrap.servers", "b:9092", "max.request.size", "2000"), "grp")));
        assertEquals(3000, KafkaReplySink.maxRecordBytes(KafkaReplySink.producerProperties(props,
                Map.of("bootstrap.servers", "b:9092", "max.request.size", 3000), "grp")));
        assertEquals(1048576, KafkaReplySink.maxRecordBytes(KafkaReplySink.producerProperties(props,
                Map.of("bootstrap.servers", "b:9092"), "grp")), "client default when not configured");
    }
}
