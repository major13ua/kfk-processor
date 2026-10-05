package xme.common.kfkprocessor.requestreply.adapters.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.LaneAccessDeniedException;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * Kafka adapter of {@link RequestLanes}: one subscription over all lane sources, static membership by worker
 * identity (ADR-0005). Reads committed data only and never commits offsets (the Cycle transaction does).
 * Wire format: record headers {@code correlation_id} and {@code request_key} (OQ-1 default, still open).
 */
public class KafkaRequestLanes implements RequestLanes, AutoCloseable {

    static final String CORRELATION_ID = IncomingRequest.CORRELATION_ID;
    static final String REQUEST_KEY = IncomingRequest.REQUEST_KEY;
    /** Synthetic header (epoch millis of the Kafka record), the consistency-lag fallback when created_at is absent. */
    public static final String RECORD_TIMESTAMP = "record_timestamp";
    /** Upper bound of one poll; with the quota check in {@link #applyPauses} it bounds the surplus buffer. */
    static final int MAX_POLL_RECORDS = 100;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final Consumer<byte[], byte[]> consumer;
    private final Map<String, String> laneBySource = new LinkedHashMap<>();
    private final Map<String, String> sourceByLane = new HashMap<>();
    /**
     * Polled but not yet returned (over a lane's quota). Kept here instead of seeking back, so a small grant
     * (burst-capped rate budget) does not cost a re-fetch per round. Dropped for revoked partitions.
     */
    private final List<IncomingRequest> buffered = new ArrayList<>();
    /**
     * Per partition, the number of times it was revoked or lost: a request carries the count of its fetch, so one
     * fetched before a revocation is told from one fetched again after the reassignment (same offset).
     */
    private final Map<TopicPartition, Long> assignments = new HashMap<>();
    private boolean paused;

    public KafkaRequestLanes(
            RequestReplyProperties properties, String bootstrapServers, String groupId, WorkerMetrics metrics) {
        this(properties, Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), groupId, metrics);
    }

    /** {@code clientProperties}: bootstrap servers and security settings shared by every Kafka client of the worker. */
    public KafkaRequestLanes(RequestReplyProperties properties, Map<String, Object> clientProperties,
            String groupId, WorkerMetrics metrics) {
        this(new KafkaConsumer<>(consumerProperties(properties, clientProperties, groupId)), properties, metrics);
    }

    /** Test seam: injected consumer. */
    KafkaRequestLanes(Consumer<byte[], byte[]> consumer, RequestReplyProperties properties, WorkerMetrics metrics) {
        this.consumer = consumer;
        for (RequestReplyProperties.Lane lane : properties.getLanes()) {
            laneBySource.put(lane.getSource(), lane.getName());
            sourceByLane.put(lane.getName(), lane.getSource());
        }
        consumer.subscribe(new ArrayList<>(laneBySource.keySet()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                Set<TopicPartition> gone = new HashSet<>(partitions);
                buffered.removeIf(r -> gone.contains(partitionOf(r)));
                gone.forEach(tp -> assignments.merge(tp, 1L, Long::sum));
                if (!partitions.isEmpty()) {
                    metrics.groupMembershipChange();
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    metrics.groupMembershipChange();
                }
            }
        });
    }

    /**
     * Maps a record to the port type. A record missing the {@code correlation_id} or {@code request_key} header
     * is surfaced with a null {@code correlationId} / {@code requestKey} (malformed), never thrown.
     */
    static IncomingRequest toIncoming(String lane, ConsumerRecord<byte[], byte[]> record) {
        return toIncoming(lane, record, 0L);
    }

    /** As {@link #toIncoming(String, ConsumerRecord)}, fetched under the partition's {@code assignment}. */
    static IncomingRequest toIncoming(String lane, ConsumerRecord<byte[], byte[]> record, long assignment) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        for (Header h : record.headers()) {
            headers.put(h.key(), h.value());
        }
        headers.put(RECORD_TIMESTAMP, Long.toString(record.timestamp()).getBytes(StandardCharsets.UTF_8));
        byte[] corr = headers.get(CORRELATION_ID);
        byte[] key = headers.get(REQUEST_KEY);
        return new IncomingRequest(
                lane,
                record.partition(),
                record.offset(),
                key == null ? null : new String(key, StandardCharsets.UTF_8),
                corr == null ? null : new String(corr, StandardCharsets.UTF_8),
                headers,
                record.value(),
                assignment);
    }

    /**
     * Keeps at most {@code quotaByLane.get(lane)} requests per lane (missing lane = 0), preserving order. A lane's
     * quota is taken in turn from each of its partitions (a prefix of each partition's requests), so no partition of
     * the lane is drained ahead of the others.
     */
    static List<IncomingRequest> limitByQuota(List<IncomingRequest> polled, Map<String, Integer> quotaByLane) {
        Map<String, Map<Integer, ArrayDeque<IncomingRequest>>> byLane = new HashMap<>();
        for (IncomingRequest r : polled) {
            byLane.computeIfAbsent(r.lane(), l -> new LinkedHashMap<>())
                    .computeIfAbsent(r.partition(), p -> new ArrayDeque<>()).add(r);
        }
        Set<IncomingRequest> taken = Collections.newSetFromMap(new IdentityHashMap<>());
        byLane.forEach((lane, partitions) -> {
            int left = quotaByLane.getOrDefault(lane, 0);
            while (left > 0 && partitions.values().stream().anyMatch(q -> !q.isEmpty())) {
                for (ArrayDeque<IncomingRequest> q : partitions.values()) {
                    if (left > 0 && !q.isEmpty()) {
                        taken.add(q.poll());
                        left--;
                    }
                }
            }
        });
        List<IncomingRequest> kept = new ArrayList<>();
        for (IncomingRequest r : polled) {
            if (taken.contains(r)) {
                kept.add(r);
            }
        }
        return kept;
    }

    static Properties consumerProperties(RequestReplyProperties properties, Map<String, Object> clientProperties,
            String groupId) {
        Properties p = new Properties();
        p.putAll(clientProperties);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, properties.getWorkerIdentity());
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) properties.getIdentityWindow().toMillis());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // a rebalance revokes only the partitions that move: a held Cycle keeps its partitions and read positions
        p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return p;
    }

    /**
     * The consumer's current group metadata (member id, generation, static instance id). The reply sink hands it to
     * the broker with the offsets, so a worker that lost its partitions in a rebalance is fenced by the group.
     * Call it only from the thread that polls (the consumer is not thread-safe).
     */
    public ConsumerGroupMetadata groupMetadata() {
        return consumer.groupMetadata();
    }

    /** Polls; an authorization failure (topic READ or group) is surfaced as {@link LaneAccessDeniedException}. */
    private ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        try {
            return consumer.poll(timeout);
        } catch (AuthorizationException | AuthenticationException e) {
            throw new LaneAccessDeniedException("request lanes denied: " + e.getClass().getSimpleName(), e);
        }
    }

    private IncomingRequest incoming(ConsumerRecord<byte[], byte[]> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        return toIncoming(laneBySource.get(record.topic()), record, assignments.getOrDefault(tp, 0L));
    }

    private TopicPartition partitionOf(IncomingRequest r) {
        return new TopicPartition(sourceByLane.get(r.lane()), r.partition());
    }

    @Override
    public boolean revokedSinceFetch(IncomingRequest request) {
        TopicPartition tp = partitionOf(request);
        return assignments.getOrDefault(tp, 0L) != request.assignment() || !consumer.assignment().contains(tp);
    }

    /**
     * Moves each retained partition's read position past the committed requests when it is behind them (a
     * position reset to the old committed offset would re-read them); a position already ahead is left alone, so
     * buffered surplus is not fetched twice.
     */
    @Override
    public void committedAfterHold(List<IncomingRequest> requests) {
        Map<TopicPartition, Long> next = new LinkedHashMap<>();
        for (IncomingRequest r : requests) {
            if (!revokedSinceFetch(r)) {
                next.merge(partitionOf(r), r.position() + 1, Math::max);
            }
        }
        next.forEach((tp, offset) -> {
            if (readPosition(tp) < offset) {
                consumer.seek(tp, offset);
            }
        });
    }

    /** The partition's read position, or -1 when it has none yet (it would be reset from the committed offset). */
    private long readPosition(TopicPartition tp) {
        try {
            return consumer.position(tp, Duration.ZERO);
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            return -1;
        }
    }

    /** Polled-but-not-returned requests (surplus over the quotas). */
    int bufferedCount() {
        return buffered.size();
    }

    @Override
    public boolean awaitAvailable() {
        if (paused) {
            return false;
        }
        if (buffered.isEmpty()) {
            consumer.resume(consumer.assignment());
            for (ConsumerRecord<byte[], byte[]> record : poll(POLL_TIMEOUT)) {
                buffered.add(incoming(record));
            }
        }
        return !buffered.isEmpty();
    }

    @Override
    public void keepAlive() {
        consumer.pause(consumer.assignment());
        // partitions assigned by a rebalance inside this poll start unpaused: rewind whatever it returned
        Map<TopicPartition, Long> rewind = new LinkedHashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : poll(Duration.ZERO)) {
            rewind.putIfAbsent(new TopicPartition(record.topic(), record.partition()), record.offset());
        }
        rewind.forEach(consumer::seek);
        consumer.pause(consumer.assignment());
    }

    @Override
    public void release(List<IncomingRequest> requests) {
        // a revoked partition's position is re-read (or moved to another member): its requests come again from there
        List<IncomingRequest> back = new ArrayList<>();
        for (IncomingRequest r : requests) {
            if (!revokedSinceFetch(r)) {
                back.add(r);
            }
        }
        buffered.addAll(0, back);
    }

    @Override
    public List<IncomingRequest> fetch(Map<String, Integer> quotaByLane) {
        applyPauses(quotaByLane);
        if (!covers(quotaByLane)) {
            for (ConsumerRecord<byte[], byte[]> record : poll(Duration.ZERO)) {
                buffered.add(incoming(record));
            }
        }
        List<IncomingRequest> kept = limitByQuota(buffered, quotaByLane);
        Set<IncomingRequest> returned = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        returned.addAll(kept);
        buffered.removeIf(returned::contains);
        return kept;
    }

    /** True when the polled-but-not-returned requests already fill every lane's quota (no poll needed). */
    private boolean covers(Map<String, Integer> quotaByLane) {
        Map<String, Integer> have = new HashMap<>();
        for (IncomingRequest r : buffered) {
            have.merge(r.lane(), 1, Integer::sum);
        }
        return !quotaByLane.isEmpty()
                && quotaByLane.entrySet().stream().allMatch(e -> have.getOrDefault(e.getKey(), 0) >= e.getValue());
    }

    /**
     * Pauses partitions that need nothing: their lane has no quota, or the buffer already holds the lane's quota
     * from that partition (so the surplus stays bounded), and all of them while paused. Counted per partition, so a
     * worker owning several partitions of a lane keeps fetching all of them instead of draining one while the
     * others wait (a drained partition would make the lane look idle and give its share away). Nothing is fetched
     * and lost.
     */
    private void applyPauses(Map<String, Integer> quotaByLane) {
        Map<TopicPartition, Integer> have = new HashMap<>();
        for (IncomingRequest r : buffered) {
            have.merge(partitionOf(r), 1, Integer::sum);
        }
        Set<TopicPartition> pause = new HashSet<>();
        Set<TopicPartition> resume = new HashSet<>();
        for (TopicPartition tp : consumer.assignment()) {
            int quota = quotaByLane.getOrDefault(laneBySource.get(tp.topic()), 0);
            boolean satisfied = quota <= 0 || have.getOrDefault(tp, 0) >= quota;
            (paused || satisfied ? pause : resume).add(tp);
        }
        consumer.pause(pause);
        consumer.resume(resume);
    }

    @Override
    public void pause() {
        paused = true;
        consumer.pause(consumer.assignment());
    }

    @Override
    public void resume() {
        paused = false;
    }

    /** Closes without leaving the group and without committing: the static member keeps its lanes for the window. */
    @Override
    public void close() {
        consumer.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP));
    }
}
