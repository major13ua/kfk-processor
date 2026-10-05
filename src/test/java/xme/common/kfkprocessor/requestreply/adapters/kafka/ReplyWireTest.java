package xme.common.kfkprocessor.requestreply.adapters.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.api.Reply;
import xme.common.kfkprocessor.requestreply.engine.CycleCommitter;
import xme.common.kfkprocessor.requestreply.engine.HandlerResult;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;

/** F2 (review B8): what a Requester sees on the wire. Header names follow the OQ-1 default (headers). */
class ReplyWireTest {

    private static final Map<String, String> SOURCES = Map.of("high", "src-high");

    private static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static KafkaReplySink sink(MockProducer<byte[], byte[]> p) {
        return new KafkaReplySink(p, "replies", SOURCES,
                new ConsumerGroupMetadata("grp", -1, "", java.util.Optional.empty()));
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String header(org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    // AC-06: an Error Reply must be told apart from a reply without parsing the body
    @Test
    void errorReplyAndReplyCarryDifferentTypeHeaders() {
        var p = producer();
        sink(p).commit(List.of(
                new ReplyRecord("high", 0, 1, "c-1", "k", b("answer"), false),
                new ReplyRecord("high", 0, 2, "c-2", "k", b("{\"category\":\"failure\"}"), true)));

        assertEquals("reply", header(p.history().get(0), "type"));
        assertEquals("error_reply", header(p.history().get(1), "type"));
    }

    // AC-08: the undeliverable Error Reply that replaces an unsendable reply is an Error Reply on the wire
    @Test
    void fallbackThatReplacesAReplyIsTypedAsErrorReply() {
        var p = producer();
        sink(p).commit(List.of(new ReplyRecord("high", 0, 1, "c-1", "k", null, false, b("undeliverable"))));

        assertEquals("error_reply", header(p.history().get(0), "type"));
    }

    // AC-01/AC-04: correlation id and Request Key are echoed unchanged, including bytes that are not UTF-8
    @Test
    void nonUtf8CorrelationIdAndKeySurviveTheRoundTripUnchanged() {
        byte[] corr = {(byte) 0xFF, (byte) 0xFE, 0x01, 'a'};
        byte[] key = {(byte) 0xC3, 0x28, 'k'};
        var record = new ConsumerRecord<>("src-high", 0, 7L, new byte[0], b("payload"));
        record.headers().add(new RecordHeader("correlation_id", corr));
        record.headers().add(new RecordHeader("request_key", key));
        IncomingRequest in = KafkaRequestLanes.toIncoming("high", record);

        // a Handler that echoes what it was given, as the contract demands
        String echoedCorr = in.correlationId();
        String echoedKey = (String) in.requestKey();
        var committer = new CycleCommitter<String, String>(sink(producerHolder),
                r -> b("answer"),
                e -> b("{\"category\":\"" + e.category().name().toLowerCase() + "\"}"));
        committer.commit(List.of(
                new HandlerResult<>(in, new Reply<>(echoedCorr, echoedKey, "answer"), null),
                new HandlerResult<>(in, null, ErrorReply.of(ErrorCategory.FAILURE, echoedCorr, echoedKey))));

        for (var sent : producerHolder.history()) {
            assertArrayEquals(corr, sent.headers().lastHeader("correlation_id").value());
            assertArrayEquals(key, sent.headers().lastHeader("request_key").value());
        }
        assertEquals(2, producerHolder.history().size());
    }

    private final MockProducer<byte[], byte[]> producerHolder = producer();
}
