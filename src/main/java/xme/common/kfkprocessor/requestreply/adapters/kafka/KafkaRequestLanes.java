package xme.common.kfkprocessor.requestreply.adapters.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

/**
 * Kafka adapter of {@link RequestLanes}: one subscription over all lane sources, static membership by worker
 * identity (ADR-0005). Reads committed data only and never commits offsets (the Cycle transaction does).
 * Wire format: record headers {@code correlation_id} and {@code request_key} (OQ-1 default, still open).
 */
public class KafkaRequestLanes implements RequestLanes, AutoCloseable {

    static final String CORRELATION_ID = "correlation_id";
    static final String REQUEST_KEY = "request_key";
    /** Synthetic header (epoch millis of the Kafka record), the consistency-lag fallback when created_at is absent. */
    public static final String RECORD_TIMESTAMP = "record_timestamp";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Map<String, String> laneBySource = new LinkedHashMap<>();
    private final Map<String, String> sourceByLane = new HashMap<>();
    /**
     * Polled but not yet returned (over a lane's quota). Kept here instead of seeking back, so a small grant
     * (burst-capped rate budget) does not cost a re-fetch per round. Dropped for revoked partitions.
     */
    private final List<IncomingRequest> buffered = new ArrayList<>();
    private boolean paused;

    public KafkaRequestLanes(
            RequestReplyProperties properties, String bootstrapServers, String groupId, WorkerMetrics metrics) {
        for (RequestReplyProperties.Lane lane : properties.getLanes()) {
            laneBySource.put(lane.getSource(), lane.getName());
            sourceByLane.put(lane.getName(), lane.getSource());
        }
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, properties.getWorkerIdentity());
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) properties.getIdentityWindow().toMillis());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumer = new KafkaConsumer<>(p);
        consumer.subscribe(new ArrayList<>(laneBySource.keySet()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                Set<TopicPartition> gone = new HashSet<>(partitions);
                buffered.removeIf(r -> gone.contains(new TopicPartition(sourceByLane.get(r.lane()), r.partition())));
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
                record.value());
    }

    /** Keeps at most {@code quotaByLane.get(lane)} requests per lane (missing lane = 0), preserving order. */
    static List<IncomingRequest> limitByQuota(List<IncomingRequest> polled, Map<String, Integer> quotaByLane) {
        Map<String, Integer> taken = new HashMap<>();
        List<IncomingRequest> kept = new ArrayList<>();
        for (IncomingRequest r : polled) {
            int n = taken.getOrDefault(r.lane(), 0);
            if (n < quotaByLane.getOrDefault(r.lane(), 0)) {
                taken.put(r.lane(), n + 1);
                kept.add(r);
            }
        }
        return kept;
    }

    @Override
    public List<IncomingRequest> fetch(Map<String, Integer> quotaByLane) {
        applyPauses(quotaByLane);
        if (!covers(quotaByLane)) {
            for (ConsumerRecord<byte[], byte[]> record : consumer.poll(POLL_TIMEOUT)) {
                buffered.add(toIncoming(laneBySource.get(record.topic()), record));
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

    /** Pauses partitions of lanes without quota (and all of them while paused) so nothing is fetched and lost. */
    private void applyPauses(Map<String, Integer> quotaByLane) {
        Set<TopicPartition> pause = new HashSet<>();
        Set<TopicPartition> resume = new HashSet<>();
        for (TopicPartition tp : consumer.assignment()) {
            String lane = laneBySource.get(tp.topic());
            boolean noQuota = quotaByLane.getOrDefault(lane, 0) <= 0;
            (paused || noQuota ? pause : resume).add(tp);
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
