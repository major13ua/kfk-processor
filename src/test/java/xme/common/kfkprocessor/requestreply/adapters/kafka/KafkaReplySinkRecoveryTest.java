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
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;

/**
 * F2 (review A1, A2): a fake producer that behaves like the real transactional client where the review claims the
 * sink goes wrong: a failed send poisons the open transaction, a commit that times out has an unknown outcome and
 * is resolved by calling commitTransaction again, and a fenced producer stays fenced.
 */
class KafkaReplySinkRecoveryTest {

    private static final String REPLIES = "replies";
    private static final Map<String, String> SOURCES = Map.of("high", "src-high");

    private static final class Txn extends MockProducer<byte[], byte[]> {
        Function<ProducerRecord<byte[], byte[]>, RuntimeException> brokerFault = r -> null;
        boolean poisoned;
        boolean timeoutOnFirstCommit;
        boolean fenceOnCommit;
        boolean commitAppliedButUnknown;
        int initCalls;
        int commitCalls;

        Txn() {
            super(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public synchronized void initTransactions() {
            if (initCalls++ == 0) {
                super.initTransactions();
            }
            commitAppliedButUnknown = false; // a new instance's init completes the old commit on the broker
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            if (poisoned) {
                throw new KafkaException("Cannot perform send because at least one previous transactional or "
                        + "idempotent request has failed with errors.");
            }
            RuntimeException f = brokerFault.apply(record);
            if (f == null) {
                return super.send(record, callback);
            }
            poisoned = true;
            if (callback != null) {
                callback.onCompletion(null, f);
            }
            return CompletableFuture.failedFuture(f);
        }

        @Override
        public synchronized void beginTransaction() {
            if (commitAppliedButUnknown) {
                throw new IllegalStateException("A commit is still in progress; call commitTransaction again");
            }
            super.beginTransaction();
        }

        @Override
        public synchronized void commitTransaction() {
            commitCalls++;
            if (commitAppliedButUnknown) {
                commitAppliedButUnknown = false; // the retry completes the commit that already happened
                return;
            }
            if (poisoned) {
                throw new KafkaException("Cannot commit a transaction in an error state");
            }
            if (fenceOnCommit) {
                throw new ProducerFencedException("fenced by a newer instance");
            }
            super.commitTransaction();
            if (timeoutOnFirstCommit) {
                timeoutOnFirstCommit = false;
                commitAppliedButUnknown = true;
                throw new TimeoutException("commit outcome unknown");
            }
        }

        @Override
        public synchronized void abortTransaction() {
            if (fenceOnCommit) {
                throw new ProducerFencedException("fenced by a newer instance");
            }
            if (commitAppliedButUnknown) {
                throw new IllegalStateException("Cannot abort while a commit is in progress");
            }
            poisoned = false;
            super.abortTransaction();
        }

        @Override
        public void close() {
        }

        @Override
        public void close(java.time.Duration timeout) {
        }
    }

    private static KafkaReplySink sink(Txn p) {
        return new KafkaReplySink(p, REPLIES, SOURCES, new ConsumerGroupMetadata("grp", -1, "", java.util.Optional.empty()));
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ReplyRecord reply(long position, String corr, byte[] value, boolean error, byte[] fallback) {
        return new ReplyRecord("high", 0, position, corr, "key-" + corr, value, error, fallback);
    }

    private static String corr(ProducerRecord<byte[], byte[]> r) {
        return new String(r.headers().lastHeader("correlation_id").value(), StandardCharsets.UTF_8);
    }

    // A1 / AC-08: the broker rejects a reply; the failed send poisons the transaction, so the fallback cannot
    // join it. The Cycle must still commit, with the fallback in place of the rejected reply.
    @Test
    void brokerRejectedReplyIsReplacedByItsFallbackAndTheCycleCommits() {
        var p = new Txn();
        p.brokerFault = r -> new String(r.value(), StandardCharsets.UTF_8).equals("huge")
                ? new RecordTooLargeException("broker: message too large") : null;

        CommitResult result = sink(p).commit(List.of(
                reply(1, "ok-1", b("a"), false, b("undeliverable-ok-1")),
                reply(2, "big", b("huge"), false, b("undeliverable-big")),
                reply(3, "ok-2", b("b"), false, b("undeliverable-ok-2"))));

        assertEquals(1, result.failures().size());
        assertTrue(result.failures().get(0).substituted());
        assertEquals(List.of("ok-1=a", "big=undeliverable-big", "ok-2=b"),
                p.history().stream().map(r -> corr(r) + "=" + new String(r.value(), StandardCharsets.UTF_8)).toList());
        assertEquals(4L, p.consumerGroupOffsetsHistory().get(0).get("grp")
                .get(new org.apache.kafka.common.TopicPartition("src-high", 0)).offset());
    }

    // A1 / AC-08: an Error Reply (no fallback) rejected by the broker is reported; the rest still commits.
    @Test
    void brokerRejectedReplyWithoutFallbackIsReportedAndTheCycleCommits() {
        var p = new Txn();
        p.brokerFault = r -> new String(r.value(), StandardCharsets.UTF_8).equals("huge")
                ? new RecordTooLargeException("broker: message too large") : null;

        CommitResult result = sink(p).commit(List.of(
                reply(1, "ok-1", b("a"), false, null),
                reply(2, "err", b("huge"), true, null)));

        assertEquals(1, result.failures().size());
        assertFalse(result.failures().get(0).substituted());
        assertEquals(List.of("ok-1"), p.history().stream().map(KafkaReplySinkRecoveryTest::corr).toList());
        assertEquals(3L, p.consumerGroupOffsetsHistory().get(0).get("grp")
                .get(new org.apache.kafka.common.TopicPartition("src-high", 0)).offset());
    }

    // A2 / AC-05: a commit that times out has an unknown outcome. The retry of the same Cycle (what the commit
    // retry and the probe-resume path do) must resolve it, never re-send the replies into a new transaction.
    @Test
    void retryAfterCommitTimeoutDoesNotCommitTheRepliesTwice() {
        var p = new Txn();
        p.timeoutOnFirstCommit = true;
        var sink = sink(p);
        List<ReplyRecord> cycle = List.of(reply(1, "c-1", b("a"), false, null));

        assertThrows(ReplyDestinationFault.class, () -> sink.commit(cycle));
        CommitResult second = sink.commit(cycle);

        assertTrue(second.failures().isEmpty());
        assertEquals(1, p.history().size(), "the reply was committed exactly once");
    }

    // A2 / AC-05: a fenced producer means a newer instance owns the transactional id; re-creating the producer
    // would fence that live instance in turn.
    @Test
    void fencedProducerIsNeverRecreated() {
        var p = new Txn();
        p.fenceOnCommit = true;
        var sink = sink(p);
        List<ReplyRecord> cycle = List.of(reply(1, "c-1", b("a"), false, null));

        assertThrows(RuntimeException.class, () -> sink.commit(cycle));
        assertThrows(RuntimeException.class, () -> sink.commit(cycle));

        assertEquals(1, p.initCalls, "initTransactions again would take the id back from the live instance");
    }
}
