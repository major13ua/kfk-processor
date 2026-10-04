package xme.common.kfkprocessor.requestreply.adapters.kafka;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.CommitResult.Reason;
import xme.common.kfkprocessor.requestreply.ports.CommitResult.ReplyFailure;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;

/**
 * Kafka adapter of {@link ReplySink}: one transaction per commit holding the replies and the next request
 * positions (ADR-0002). The transactional id derives from the worker identity (ADR-0005) and the transaction
 * timeout equals the commit window. A failure specific to one reply is reported, never aborts the transaction;
 * when the reply has a fallback Error Reply, that is sent in its place in the same transaction.
 * A producer that failed fatally (for example fenced by a newer instance) is discarded and re-created on the
 * next commit.
 */
public class KafkaReplySink implements ReplySink, AutoCloseable {

    private static final String CORRELATION_ID = "correlation_id";
    private static final String REQUEST_KEY = "request_key";

    private final Supplier<Producer<byte[], byte[]>> factory;
    private final String replyTopic;
    private final Map<String, String> sourceByLane;
    private final ConsumerGroupMetadata group;
    private int maxRecordBytes = Integer.MAX_VALUE;
    private Producer<byte[], byte[]> producer;
    private boolean initialized;

    /** Production constructor: builds the transactional producer from the properties. */
    public KafkaReplySink(RequestReplyProperties properties, String bootstrapServers, String groupId) {
        this(() -> newProducer(properties, bootstrapServers), properties.getReplyDestination(),
                sources(properties), new ConsumerGroupMetadata(groupId, -1, "", java.util.Optional.empty()));
        this.maxRecordBytes = ProducerConfig.configDef().defaultValues().get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG) instanceof Integer i
                ? i : Integer.MAX_VALUE;
    }

    /** Test seam: injected producer, reply topic, lane name to source topic, consumer group of the positions. */
    KafkaReplySink(Producer<byte[], byte[]> producer, String replyTopic, Map<String, String> sourceByLane,
            ConsumerGroupMetadata group) {
        this(() -> producer, replyTopic, sourceByLane, group);
    }

    private KafkaReplySink(Supplier<Producer<byte[], byte[]>> factory, String replyTopic,
            Map<String, String> sourceByLane, ConsumerGroupMetadata group) {
        this.factory = factory;
        this.replyTopic = replyTopic;
        this.sourceByLane = sourceByLane;
        this.group = group;
    }

    /** Transactional id derived from the worker identity (ADR-0005). */
    static String transactionalId(String workerIdentity) {
        return "request-reply-" + workerIdentity;
    }

    private static Map<String, String> sources(RequestReplyProperties properties) {
        Map<String, String> m = new HashMap<>();
        for (RequestReplyProperties.Lane lane : properties.getLanes()) {
            m.put(lane.getName(), lane.getSource());
        }
        return m;
    }

    private static Producer<byte[], byte[]> newProducer(RequestReplyProperties properties, String bootstrapServers) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId(properties.getWorkerIdentity()));
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        int timeoutMs = (int) properties.getCommitWindow().toMillis();
        p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, timeoutMs);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, timeoutMs);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    @Override
    public synchronized CommitResult commit(List<ReplyRecord> replies) {
        List<ReplyFailure> failures = new ArrayList<>();
        Producer<byte[], byte[]> p = null;
        boolean began = false;
        try {
            p = producer();
            p.beginTransaction();
            began = true;
            boolean[] substituted = new boolean[replies.size()];
            List<Exception> errors = send(p, replies, substituted);
            for (int i = 0; i < replies.size(); i++) {
                Exception e = errors.get(i);
                if (e != null) {
                    failures.add(failure(replies.get(i), perReply(e), substituted[i]));
                }
            }
            p.sendOffsetsToTransaction(nextPositions(replies), group);
            p.commitTransaction();
            return new CommitResult(failures);
        } catch (ReplyDestinationFault fault) {
            abort(p, began);
            throw fault;
        } catch (RuntimeException e) {
            abort(p, began);
            throw translate(e);
        }
    }

    /**
     * Sends all replies; returns the per-reply send error (null = sent or substituted). A reply that fails for a
     * per-reply reason and has a fallback is replaced by the fallback in the same transaction; its entry in
     * {@code substituted} is set. Destination faults are thrown.
     */
    private List<Exception> send(Producer<byte[], byte[]> p, List<ReplyRecord> replies, boolean[] substituted) {
        int n = replies.size();
        Exception[] errors = new Exception[n];
        for (int i = 0; i < n; i++) {
            sendOne(p, errors, i, replies.get(i).value(), replies.get(i));
        }
        p.flush();
        checkFaults(errors);
        Exception[] primary = errors.clone();
        boolean any = false;
        for (int i = 0; i < n; i++) {
            if (primary[i] != null && replies.get(i).fallback() != null) {
                errors[i] = null;
                substituted[i] = true;
                sendOne(p, errors, i, replies.get(i).fallback(), replies.get(i));
                any = true;
            }
        }
        if (any) {
            p.flush();
            checkFaults(errors);
        }
        List<Exception> result = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (substituted[i] && errors[i] != null) {
                substituted[i] = false; // the fallback failed too: report the original failure
                errors[i] = primary[i];
            }
            result.add(errors[i] != null ? errors[i] : (substituted[i] ? primary[i] : null));
        }
        return result;
    }

    private void sendOne(Producer<byte[], byte[]> p, Exception[] errors, int i, byte[] value, ReplyRecord r) {
        if (value == null) {
            errors[i] = new IllegalArgumentException("reply cannot be encoded");
            return;
        }
        ProducerRecord<byte[], byte[]> rec = record(r, value);
        if (exceedsClientLimit(rec)) {
            // the client would fail the whole transaction on an oversized record, so it is never sent
            errors[i] = new RecordTooLargeException("reply exceeds max.request.size");
            return;
        }
        try {
            p.send(rec, (metadata, e) -> {
                if (e != null) {
                    errors[i] = e;
                }
            });
        } catch (RuntimeException e) {
            errors[i] = e;
        }
        if (errors[i] != null && perReply(errors[i]) == null) {
            throw destinationFault(errors[i]);
        }
    }

    private static void checkFaults(Exception[] errors) {
        for (Exception e : errors) {
            if (e != null && perReply(e) == null) {
                throw destinationFault(e);
            }
        }
    }

    private ProducerRecord<byte[], byte[]> record(ReplyRecord r, byte[] value) {
        ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(replyTopic, null, value);
        rec.headers().add(new RecordHeader(CORRELATION_ID, utf8(r.correlationId())));
        rec.headers().add(new RecordHeader(REQUEST_KEY, keyBytes(r.requestKey())));
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
        if (e instanceof AuthorizationException && !(e instanceof org.apache.kafka.common.errors.TransactionalIdAuthorizationException)
                || e instanceof TopicAuthorizationException) {
            return new ReplyDestinationFault.PermissionDenied("reply destination denies access", e);
        }
        return new ReplyDestinationFault.Unavailable("reply destination unavailable", e);
    }

    /** Destination faults become typed faults; everything else (fenced, commit failure) propagates untyped. */
    private RuntimeException translate(RuntimeException e) {
        if (e instanceof AuthorizationException && !(e instanceof org.apache.kafka.common.errors.TransactionalIdAuthorizationException)) {
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

    /** Aborts the open transaction; a producer that cannot abort (fenced, fatal) is discarded for re-creation. */
    private void abort(Producer<byte[], byte[]> p, boolean began) {
        if (p == null) {
            return;
        }
        if (began) {
            try {
                p.abortTransaction();
                return;
            } catch (RuntimeException ignored) {
                // fall through: discard the producer
            }
        } else {
            discard();
            return;
        }
        discard();
    }

    private void discard() {
        Producer<byte[], byte[]> p = producer;
        producer = null;
        initialized = false;
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
