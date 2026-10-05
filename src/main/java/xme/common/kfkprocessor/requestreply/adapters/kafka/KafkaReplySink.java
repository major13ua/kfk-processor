package xme.common.kfkprocessor.requestreply.adapters.kafka;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.CommitResult.Reason;
import xme.common.kfkprocessor.requestreply.ports.CommitResult.ReplyFailure;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;

/**
 * Kafka adapter of {@link ReplySink}: one transaction per commit holding the replies and the next request
 * positions (ADR-0002). The transactional id derives from the consumer group and the worker identity (ADR-0005)
 * and the transaction timeout equals the commit window.
 *
 * <p>A failure specific to one reply never loses the Cycle. A failed send poisons the open transaction on a real
 * client, so such a reply is moved to its fallback Error Reply (or dropped when it has none) and the Cycle is
 * written again in a fresh transaction after aborting the poisoned one; the positions commit once, in the last.
 *
 * <p>A commit that times out has an unknown outcome. The producer is kept and the next call for the same Cycle
 * calls {@code commitTransaction} again instead of sending the replies into a new transaction (which would
 * duplicate them if the first commit had gone through). A producer that failed otherwise is discarded and
 * re-created on the next commit, except a fenced one: a newer instance owns the transactional id, and taking it
 * back would fence that live instance in turn.
 */
public class KafkaReplySink implements ReplySink, AutoCloseable {

    private static final String CORRELATION_ID = IncomingRequest.CORRELATION_ID;
    private static final String REQUEST_KEY = IncomingRequest.REQUEST_KEY;
    /** Wire: record header telling a reply from an Error Reply; value {@link #TYPE_REPLY} or {@link #TYPE_ERROR_REPLY}. */
    static final String TYPE = "type";
    static final String TYPE_REPLY = "reply";
    static final String TYPE_ERROR_REPLY = "error_reply";

    private static final int PRIMARY = 0;
    private static final int FALLBACK = 1;
    private static final int DROPPED = 2;

    private final Supplier<Producer<byte[], byte[]>> factory;
    private final String replyTopic;
    private final Map<String, String> sourceByLane;
    private final Supplier<ConsumerGroupMetadata> group;
    private int maxRecordBytes = Integer.MAX_VALUE;
    private Producer<byte[], byte[]> producer;
    private boolean initialized;
    private boolean open;
    private boolean fenced;
    /** Identity of the Cycle whose commit timed out (outcome unknown), and what to answer once it resolves. */
    private List<String> pendingCycle;
    private CommitResult pendingResult;

    /**
     * Production constructor: builds the transactional producer from the properties. {@code groupMetadata} gives
     * the lanes consumer's live group metadata (see {@code KafkaRequestLanes#groupMetadata()}); when null the
     * offsets are sent for the group id alone, which the group cannot fence.
     */
    public KafkaReplySink(RequestReplyProperties properties, String bootstrapServers, String groupId,
            Supplier<ConsumerGroupMetadata> groupMetadata) {
        this(properties, Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), groupId, groupMetadata);
    }

    /** {@code clientProperties}: bootstrap servers and security settings shared by every Kafka client of the worker. */
    public KafkaReplySink(RequestReplyProperties properties, Map<String, Object> clientProperties, String groupId,
            Supplier<ConsumerGroupMetadata> groupMetadata) {
        this(() -> new KafkaProducer<>(producerProperties(properties, clientProperties, groupId)),
                properties.getReplyDestination(),
                sources(properties), groupMetadata != null ? groupMetadata : unfenced(groupId));
        this.maxRecordBytes = ProducerConfig.configDef().defaultValues().get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG) instanceof Integer i
                ? i : Integer.MAX_VALUE;
    }

    /** Production constructor without a lanes consumer: no group fencing. */
    public KafkaReplySink(RequestReplyProperties properties, String bootstrapServers, String groupId) {
        this(properties, bootstrapServers, groupId, null);
    }

    /** Test seam: injected producer, reply topic, lane name to source topic, consumer group of the positions. */
    KafkaReplySink(Producer<byte[], byte[]> producer, String replyTopic, Map<String, String> sourceByLane,
            ConsumerGroupMetadata group) {
        this(() -> producer, replyTopic, sourceByLane, () -> group);
    }

    /** Test seam: as above, with the group metadata read at every commit. */
    KafkaReplySink(Producer<byte[], byte[]> producer, String replyTopic, Map<String, String> sourceByLane,
            Supplier<ConsumerGroupMetadata> group) {
        this(() -> producer, replyTopic, sourceByLane, group);
    }

    private KafkaReplySink(Supplier<Producer<byte[], byte[]>> factory, String replyTopic,
            Map<String, String> sourceByLane, Supplier<ConsumerGroupMetadata> group) {
        this.factory = factory;
        this.replyTopic = replyTopic;
        this.sourceByLane = sourceByLane;
        this.group = group;
    }

    @SuppressWarnings("removal")
    private static Supplier<ConsumerGroupMetadata> unfenced(String groupId) {
        ConsumerGroupMetadata m = new ConsumerGroupMetadata(groupId, -1, "", java.util.Optional.empty());
        return () -> m;
    }

    /**
     * Transactional id derived from the consumer group and the worker identity (ADR-0005). The group is part of
     * it so the same identity in another group never shares, and so fences, this worker's id. Length-prefixed, so
     * no two (group, identity) pairs give the same id.
     */
    public static String transactionalId(String group, String workerIdentity) {
        return "request-reply-" + group.length() + ":" + group + ":" + workerIdentity;
    }

    private static Map<String, String> sources(RequestReplyProperties properties) {
        Map<String, String> m = new HashMap<>();
        for (RequestReplyProperties.Lane lane : properties.getLanes()) {
            m.put(lane.getName(), lane.getSource());
        }
        return m;
    }

    static Properties producerProperties(RequestReplyProperties properties, Map<String, Object> clientProperties,
            String groupId) {
        Properties p = new Properties();
        p.putAll(clientProperties);
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId(groupId, properties.getWorkerIdentity()));
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        int timeoutMs = (int) properties.getCommitWindow().toMillis();
        p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, timeoutMs);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, timeoutMs);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return p;
    }

    @Override
    public synchronized CommitResult commit(List<ReplyRecord> replies) {
        Producer<byte[], byte[]> p = null;
        try {
            if (fenced) {
                throw new ProducerFencedException("fenced by a newer instance with the same transactional id;"
                        + " not re-created, so the live instance keeps it");
            }
            p = producer();
            if (pendingCycle != null) {
                p.commitTransaction(); // resolves the commit that timed out; throws again while still unknown
                CommitResult done = pendingResult;
                boolean sameCycle = pendingCycle.equals(cycleId(replies));
                pendingCycle = null;
                pendingResult = null;
                if (sameCycle) {
                    return done;
                }
            }
            return transact(p, replies);
        } catch (ReplyDestinationFault fault) {
            recover(p, fault);
            throw fault;
        } catch (RuntimeException e) {
            recover(p, e);
            throw translate(e);
        }
    }

    private void recover(Producer<byte[], byte[]> p, RuntimeException e) {
        if (isFenced(e)) {
            fenced = true;
            discard();
        } else if (pendingCycle == null) {
            abort(p);
        }
    }

    private static boolean isFenced(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ProducerFencedException || t instanceof InvalidProducerEpochException) {
                return true;
            }
        }
        return false;
    }

    private static List<String> cycleId(List<ReplyRecord> replies) {
        List<String> id = new ArrayList<>(replies.size());
        for (ReplyRecord r : replies) {
            id.add(r.lane() + ":" + r.partition() + ":" + r.position());
        }
        return id;
    }

    /**
     * Writes the Cycle in as many transactions as it takes: a reply that fails for a per-reply reason moves to its
     * fallback, then is dropped, and the transaction is aborted and written again. Every round demotes at least one
     * reply, so it ends. Destination faults are thrown.
     */
    private CommitResult transact(Producer<byte[], byte[]> p, List<ReplyRecord> replies) {
        int n = replies.size();
        int[] stage = new int[n];
        Exception[] first = new Exception[n]; // the failure that moved the reply off its primary value
        while (true) {
            p.beginTransaction();
            open = true;
            AtomicReferenceArray<Exception> async = new AtomicReferenceArray<>(n);
            boolean restart = sendRound(p, replies, stage, first, async);
            if (!restart) {
                p.flush();
                for (int i = 0; i < n; i++) {
                    Exception e = async.get(i);
                    if (e != null) {
                        demote(i, e, stage, first, replies);
                        restart = true;
                    }
                }
            }
            if (restart) {
                open = false;
                p.abortTransaction();
                continue;
            }
            p.sendOffsetsToTransaction(nextPositions(replies), group.get());
            CommitResult result = new CommitResult(failures(replies, stage, first));
            try {
                p.commitTransaction();
            } catch (org.apache.kafka.common.errors.TimeoutException e) {
                // unknown outcome: keep the producer, resolve by committing again, never re-send
                pendingCycle = cycleId(replies);
                pendingResult = result;
                open = false;
                throw destinationFault(e);
            }
            open = false;
            return result;
        }
    }

    /** Sends every reply at its stage; true when one failed on send and the transaction has to be redone. */
    private boolean sendRound(Producer<byte[], byte[]> p, List<ReplyRecord> replies, int[] stage, Exception[] first,
            AtomicReferenceArray<Exception> async) {
        for (int i = 0; i < replies.size(); i++) {
            ReplyRecord r = replies.get(i);
            while (stage[i] < DROPPED) {
                boolean fallback = stage[i] == FALLBACK;
                byte[] value = fallback ? r.fallback() : r.value();
                if (fallback && value == null) {
                    stage[i] = DROPPED; // no fallback: only the original failure is reported
                    break;
                }
                Exception clientSide = clientSideFailure(r, value, fallback);
                if (clientSide != null) {
                    demote(i, clientSide, stage, first, replies); // nothing was sent: the transaction is healthy
                    continue;
                }
                try {
                    int index = i;
                    p.send(record(r, value, fallback), (metadata, e) -> {
                        if (e != null) {
                            async.set(index, e);
                        }
                    });
                    break;
                } catch (RuntimeException e) {
                    if (perReply(e) == null) {
                        if (failedAsynchronously(p, async)) {
                            return false; // an earlier send failed and poisoned this transaction: send refuses now
                        }
                        throw destinationFault(e);
                    }
                    demote(i, e, stage, first, replies);
                    return true; // a failed send may have poisoned the transaction
                }
            }
        }
        return false;
    }

    /** True when an earlier send of the round has failed (its callback ran once the producer is flushed). */
    private static boolean failedAsynchronously(Producer<byte[], byte[]> p, AtomicReferenceArray<Exception> async) {
        try {
            p.flush();
        } catch (RuntimeException ignored) {
            return false;
        }
        for (int i = 0; i < async.length(); i++) {
            if (async.get(i) != null) {
                return true;
            }
        }
        return false;
    }

    /** Moves reply {@code i} to its next stage; a destination fault is thrown instead. */
    private static void demote(int i, Exception e, int[] stage, Exception[] first, List<ReplyRecord> replies) {
        if (perReply(e) == null) {
            throw destinationFault(e);
        }
        if (stage[i] == PRIMARY) {
            first[i] = e;
        }
        stage[i]++;
    }

    private List<ReplyFailure> failures(List<ReplyRecord> replies, int[] stage, Exception[] first) {
        List<ReplyFailure> failures = new ArrayList<>();
        for (int i = 0; i < replies.size(); i++) {
            if (first[i] != null) {
                failures.add(failure(replies.get(i), perReply(first[i]), stage[i] == FALLBACK));
            }
        }
        return failures;
    }

    private Exception clientSideFailure(ReplyRecord r, byte[] value, boolean fallback) {
        if (value == null) {
            return new IllegalArgumentException("reply cannot be encoded");
        }
        if (exceedsClientLimit(record(r, value, fallback))) {
            // the client would fail the whole transaction on an oversized record, so it is never sent
            return new RecordTooLargeException("reply exceeds max.request.size");
        }
        return null;
    }

    private ProducerRecord<byte[], byte[]> record(ReplyRecord r, byte[] value, boolean fallback) {
        ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(replyTopic, null, value);
        rec.headers().add(new RecordHeader(CORRELATION_ID,
                r.correlationIdBytes() != null ? r.correlationIdBytes() : utf8(r.correlationId())));
        rec.headers().add(new RecordHeader(REQUEST_KEY,
                r.requestKeyBytes() != null ? r.requestKeyBytes() : keyBytes(r.requestKey())));
        rec.headers().add(new RecordHeader(TYPE, utf8(r.error() || fallback ? TYPE_ERROR_REPLY : TYPE_REPLY)));
        return rec;
    }

    private boolean exceedsClientLimit(ProducerRecord<byte[], byte[]> rec) {
        org.apache.kafka.common.header.Header[] headers = rec.headers().toArray();
        return org.apache.kafka.common.record.AbstractRecords.estimateSizeInBytesUpperBound(
                org.apache.kafka.common.record.RecordBatch.CURRENT_MAGIC_VALUE,
                org.apache.kafka.common.compress.Compression.NONE.type(), rec.key(), rec.value(), headers)
                > maxRecordBytes;
    }

    private static byte[] keyBytes(Object key) {
        if (key instanceof byte[] b) {
            return b;
        }
        return key == null ? null : utf8(key.toString());
    }

    private static byte[] utf8(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    /** Per-request reason, or null when the error concerns the destination rather than one reply. */
    private static Reason perReply(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return Reason.UNENCODABLE;
        }
        if (e instanceof RecordTooLargeException) {
            return Reason.TOO_LARGE;
        }
        if (e instanceof org.apache.kafka.common.errors.SerializationException) {
            return Reason.UNENCODABLE;
        }
        if (e instanceof org.apache.kafka.common.errors.CorruptRecordException
                || e instanceof org.apache.kafka.common.errors.InvalidTopicException) {
            return Reason.REJECTED;
        }
        return null;
    }

    private static ReplyFailure failure(ReplyRecord r, Reason reason, boolean substituted) {
        return new ReplyFailure(r.lane(), r.partition(), r.position(), r.correlationId(), reason, substituted);
    }

    private static ReplyDestinationFault destinationFault(Throwable e) {
        if (e instanceof AuthorizationException) {
            return new ReplyDestinationFault.PermissionDenied("reply destination denies access", e);
        }
        return new ReplyDestinationFault.Unavailable("reply destination unavailable", e);
    }

    /** Destination faults become typed faults; everything else (fenced, commit failure) propagates untyped. */
    private RuntimeException translate(RuntimeException e) {
        if (e instanceof AuthorizationException) {
            return destinationFault(e);
        }
        if (e instanceof org.apache.kafka.common.errors.TimeoutException
                || e instanceof org.apache.kafka.common.errors.RetriableException
                || e instanceof org.apache.kafka.common.errors.InterruptException) {
            return destinationFault(e);
        }
        return e;
    }

    private Map<TopicPartition, OffsetAndMetadata> nextPositions(List<ReplyRecord> replies) {
        Map<TopicPartition, OffsetAndMetadata> next = new HashMap<>();
        for (ReplyRecord r : replies) {
            TopicPartition tp = new TopicPartition(sourceByLane.get(r.lane()), r.partition());
            long offset = r.position() + 1;
            next.merge(tp, new OffsetAndMetadata(offset),
                    (a, b) -> a.offset() >= b.offset() ? a : b);
        }
        return next;
    }

    private Producer<byte[], byte[]> producer() {
        if (producer == null) {
            producer = factory.get();
            initialized = false;
        }
        if (!initialized) {
            producer.initTransactions();
            initialized = true;
        }
        return producer;
    }

    /** Aborts the open transaction; a producer that cannot abort (fatal state) is discarded for re-creation. */
    private void abort(Producer<byte[], byte[]> p) {
        if (p == null) {
            return;
        }
        if (open) {
            open = false;
            try {
                p.abortTransaction();
                return;
            } catch (RuntimeException ignored) {
                // fall through: discard the producer
            }
        }
        discard();
    }

    private void discard() {
        Producer<byte[], byte[]> p = producer;
        producer = null;
        initialized = false;
        open = false;
        if (p != null) {
            try {
                p.close(java.time.Duration.ZERO);
            } catch (RuntimeException ignored) {
                // already unusable
            }
        }
    }

    @Override
    public synchronized void close() {
        discard();
    }
}
