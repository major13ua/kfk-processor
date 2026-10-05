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
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TransactionAbortableException;
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
        /** Failures thrown by the next commitTransaction calls, in order, before anything is applied. */
        final java.util.ArrayList<RuntimeException> commitFailures = new java.util.ArrayList<>();
        /** Like a real producer after InvalidProducerEpoch: unusable (cannot abort) until re-initialised. */
        boolean broken;

        Txn() {
            super(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public synchronized void initTransactions() {
            if (initCalls++ == 0) {
                super.initTransactions();
            } else if (transactionInFlight()) {
                super.abortTransaction(); // a new instance's init aborts the old open transaction on the broker
            }
            broken = false;
            commitAppliedButUnknown = false; // a new instance's init completes the old commit on the broker
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            if (broken) {
                throw new KafkaException("producer is in a fatal state");
            }
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
            if (broken) {
                throw new KafkaException("producer is in a fatal state");
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
            if (poisoned || broken) {
                throw new KafkaException("Cannot commit a transaction in an error state");
            }
            if (!commitFailures.isEmpty()) {
                RuntimeException f = commitFailures.remove(0);
                // as the real client: an abortable error needs abortTransaction, a fatal one a new producer
                poisoned = f instanceof TransactionAbortableException;
                broken = f instanceof InvalidProducerEpochException
                        || f instanceof org.apache.kafka.common.errors.InvalidTxnStateException;
                throw f;
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
            if (broken) {
                throw new KafkaException("Cannot abort in a fatal state");
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

    // review r2 A1 (F12): while held after a commit timeout, a rebalance revokes one partition and the engine drops
    // its results; the retry with the rest is still the same commit and must not send the retained replies again
    @Test
    void retryOfATimedOutCycleWithRevokedPartitionsDroppedDoesNotCommitTheRestTwice() {
        var p = new Txn();
        p.timeoutOnFirstCommit = true;
        var sink = sink(p);
        ReplyRecord retained = reply(1, "c-1", b("a"), false, null);
        ReplyRecord revoked = new ReplyRecord("high", 1, 7, "c-2", "key-c-2", b("b"), false, null);

        assertThrows(ReplyDestinationFault.class, () -> sink.commit(List.of(retained, revoked)));
        CommitResult second = sink.commit(List.of(retained));

        assertTrue(second.failures().isEmpty());
        assertEquals(2, p.history().size(), "the first commit went through; nothing was sent again");
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

    // ---- review r2 A2 (task F13) ----

    /** Calls commit for the same Cycle until it returns; fails the test when it never does within {@code calls}. */
    private static CommitResult commitWithin(KafkaReplySink sink, List<ReplyRecord> cycle, int calls) {
        RuntimeException last = null;
        for (int i = 0; i < calls; i++) {
            try {
                return sink.commit(cycle);
            } catch (RuntimeException e) {
                last = e;
            }
        }
        throw new AssertionError("the sink never recovered in " + calls + " calls; last failure: " + last, last);
    }

    private static final class FakeClock extends java.time.Clock {
        java.time.Instant now = java.time.Instant.parse("2026-01-01T00:00:00Z");
        @Override public java.time.Instant instant() { return now; }
        @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
        @Override public java.time.Clock withZone(java.time.ZoneId z) { return this; }
    }

    private static final class Engine {
        final FakeClock clock = new FakeClock();
        final java.util.List<xme.common.kfkprocessor.requestreply.engine.CommitRetry.Alert> alerts = new java.util.ArrayList<>();
        final xme.common.kfkprocessor.requestreply.engine.WorkerState state =
                new xme.common.kfkprocessor.requestreply.engine.WorkerState(clock, java.time.Duration.ofSeconds(60),
                        (xme.common.kfkprocessor.requestreply.ports.WorkerMetrics) java.lang.reflect.Proxy.newProxyInstance(
                                xme.common.kfkprocessor.requestreply.ports.WorkerMetrics.class.getClassLoader(),
                                new Class<?>[] {xme.common.kfkprocessor.requestreply.ports.WorkerMetrics.class},
                                (proxy, m, a) -> null));
        final xme.common.kfkprocessor.requestreply.engine.CommitRetry<String, String> retry;

        Engine(KafkaReplySink sink) {
            var committer = new xme.common.kfkprocessor.requestreply.engine.CycleCommitter<String, String>(sink,
                    r -> b("reply|" + r.correlationId()), e -> b("error|" + e.correlationId()));
            retry = new xme.common.kfkprocessor.requestreply.engine.CommitRetry<>(committer, () -> { }, state, clock,
                    java.time.Duration.ofSeconds(1), 1, alerts::add);
        }

        java.util.List<xme.common.kfkprocessor.requestreply.engine.HandlerResult<String, String>> cycle() {
            var q = new xme.common.kfkprocessor.requestreply.ports.IncomingRequest("high", 0, 7, "key-c1", "c1",
                    Map.of(), b("p"));
            return List.of(new xme.common.kfkprocessor.requestreply.engine.HandlerResult<>(q,
                    new xme.common.kfkprocessor.requestreply.api.Reply<>("c1", "key-c1", "r1"), null));
        }

        /** Commit, then probe-driven re-commits (one tick per probe interval) while held; returns the tick count. */
        int driveUntilResumed(int maxTicks) {
            retry.commit(cycle());
            int ticks = 0;
            while (retry.holding() && ticks < maxTicks) {
                clock.now = clock.now.plusSeconds(2);
                retry.tick();
                ticks++;
            }
            return ticks;
        }
    }

    // A2(a) / AC-08b: the commit times out (unknown), the retried commit then fails definitively. The sink must
    // clear its pending state, abort or discard, and send the Cycle again in a fresh transaction.
    @Test
    void retriedCommitFailingDefinitivelyIsResentInAFreshTransaction() {
        var p = new Txn();
        p.commitFailures.add(new TimeoutException("commit outcome unknown"));
        p.commitFailures.add(new TransactionAbortableException("transaction must be aborted"));
        var sink = sink(p);
        List<ReplyRecord> cycle = List.of(reply(1, "c-1", b("a"), false, null));

        CommitResult result = commitWithin(sink, cycle, 3);

        assertTrue(result.failures().isEmpty());
        assertEquals(List.of("c-1"), p.history().stream().map(KafkaReplySinkRecoveryTest::corr).toList(),
                "the reply is committed exactly once");
    }

    // A2(a): through the engine: after the definitive failure of the retried commit the worker must not stay paused.
    @Test
    void workerResumesAfterTheRetriedCommitFailedDefinitively() {
        var p = new Txn();
        p.commitFailures.add(new TimeoutException("commit outcome unknown"));
        p.commitFailures.add(new TransactionAbortableException("transaction must be aborted"));
        var engine = new Engine(sink(p));

        int ticks = engine.driveUntilResumed(4);

        assertFalse(engine.retry.holding(), "still holding after " + ticks + " probe ticks: the sink is stuck");
        assertEquals(1, p.history().size(), "one committed reply");
    }

    // A2(a): INVALID_TXN_STATE-style failure of the retried commit is also not permanent.
    @Test
    void workerResumesAfterTheRetriedCommitFailedWithInvalidTxnState() {
        var p = new Txn();
        p.commitFailures.add(new TimeoutException("commit outcome unknown"));
        p.commitFailures.add(new org.apache.kafka.common.errors.InvalidTxnStateException("invalid txn state"));
        var engine = new Engine(sink(p));

        int ticks = engine.driveUntilResumed(4);

        assertFalse(engine.retry.holding(), "still holding after " + ticks + " probe ticks: the sink is stuck");
        assertEquals(1, p.history().size(), "one committed reply");
    }

    // A2(b): InvalidProducerEpoch is the broker aborting a timed-out transaction: abortable, not a fence.
    @Test
    void invalidProducerEpochOnCommitIsAbortableAndTheCycleIsResent() {
        var p = new Txn();
        p.commitFailures.add(new InvalidProducerEpochException("transaction timed out and was aborted"));
        var sink = sink(p);
        List<ReplyRecord> cycle = List.of(reply(1, "c-1", b("a"), false, null));

        CommitResult result = commitWithin(sink, cycle, 2);

        assertTrue(result.failures().isEmpty());
        assertEquals(1, p.history().size(), "one committed reply");
        assertTrue(p.initCalls >= 2, "the broken producer was re-initialised");
    }

    // A2(b): same, after an unknown-outcome commit whose retry reports the epoch failure.
    @Test
    void workerResumesAfterTheRetriedCommitFailedWithInvalidProducerEpoch() {
        var p = new Txn();
        p.commitFailures.add(new TimeoutException("commit outcome unknown"));
        p.commitFailures.add(new InvalidProducerEpochException("transaction timed out and was aborted"));
        var engine = new Engine(sink(p));

        int ticks = engine.driveUntilResumed(4);

        assertFalse(engine.retry.holding(), "still holding after " + ticks + " probe ticks: the sink is stuck");
        assertEquals(1, p.history().size(), "one committed reply");
    }

    // A2(b): a real fence (ProducerFencedException) is its own alert, distinct from destination unavailable and
    // from permission denied, and is never mistaken for an epoch abort.
    @Test
    void realFenceRaisesItsOwnAlert() {
        var p = new Txn();
        p.fenceOnCommit = true;
        var engine = new Engine(sink(p));

        engine.retry.commit(engine.cycle());

        assertFalse(engine.alerts.isEmpty(), "an alert is raised");
        String id = engine.alerts.get(0).faultId();
        assertTrue(id.contains("fenced"), "fence alert id names the fence, got: " + id);
        assertFalse(id.endsWith("reply_destination.unavailable"), "not the generic destination alert: " + id);
    }

    // A2(b): an epoch abort must not raise the fence alert.
    @Test
    void invalidProducerEpochDoesNotRaiseTheFenceAlert() {
        var p = new Txn();
        p.commitFailures.add(new InvalidProducerEpochException("transaction timed out and was aborted"));
        var engine = new Engine(sink(p));

        engine.driveUntilResumed(4);

        assertTrue(engine.alerts.stream().noneMatch(a -> a.faultId().contains("fenced")),
                "no fence alert for an epoch abort: " + engine.alerts);
        assertFalse(engine.retry.holding(), "resumed");
    }
}
