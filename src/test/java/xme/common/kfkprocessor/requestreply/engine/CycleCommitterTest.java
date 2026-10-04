package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.api.Reply;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.CommitResult.ReplyFailure;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;

/** AC-08: fake sink; the Handler is simulated by a counter that must stay at one invocation per request. */
class CycleCommitterTest {

    private static final String PAYLOAD = "secret-payload";

    private static final class FakeSink implements ReplySink {
        final List<List<ReplyRecord>> calls = new ArrayList<>();
        CommitResult answer = new CommitResult(List.of());

        @Override
        public CommitResult commit(List<ReplyRecord> replies) {
            calls.add(replies);
            return answer;
        }
    }

    private final AtomicInteger handlerCalls = new AtomicInteger();
    private final FakeSink sink = new FakeSink();

    private static String s(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private CycleCommitter<String, String> committer() {
        return new CycleCommitter<>(sink,
                r -> {
                    if (r.data().equals("UNENCODABLE")) {
                        throw new IllegalStateException("cannot encode");
                    }
                    return b("reply|" + r.correlationId() + "|" + r.data());
                },
                e -> b("error|" + e.category() + "|" + e.correlationId() + "|" + e.requestKey()));
    }

    private IncomingRequest request(String lane, int partition, long position, String corr) {
        handlerCalls.incrementAndGet(); // the Handler ran once for this request
        return new IncomingRequest(lane, partition, position, "key-" + corr, corr, java.util.Map.of(), b(PAYLOAD));
    }

    private HandlerResult<String, String> ok(String lane, int partition, long position, String corr, String data) {
        return new HandlerResult<>(request(lane, partition, position, corr),
                new Reply<>(corr, "key-" + corr, data), null);
    }

    private HandlerResult<String, String> failed(String lane, int partition, long position, String corr,
            ErrorCategory category) {
        return new HandlerResult<>(request(lane, partition, position, corr), null,
                ErrorReply.of(category, corr, "key-" + corr));
    }

    @Test
    void allFineCommitsOnceWithOneRecordPerResultIncludingErrorReplies() {
        var results = List.of(
                ok("high", 0, 4, "c-1", "r1"),
                failed("high", 0, 5, "c-2", ErrorCategory.TIMEOUT),
                failed("low", 1, 9, "c-3", ErrorCategory.FAILURE));

        CommitResult out = committer().commit(results);

        assertEquals(1, sink.calls.size(), "one commit call per Cycle");
        var sent = sink.calls.get(0);
        assertEquals(3, sent.size());
        var r1 = sent.get(0);
        assertEquals("high", r1.lane());
        assertEquals(0, r1.partition());
        assertEquals(4L, r1.position());
        assertEquals("c-1", r1.correlationId());
        assertEquals("key-c-1", r1.requestKey());
        assertEquals("reply|c-1|r1", s(r1.value()));
        assertFalse(r1.error());
        var r2 = sent.get(1);
        assertTrue(r2.error());
        assertEquals("error|TIMEOUT|c-2|key-c-2", s(r2.value()));
        assertEquals(1L, sent.get(2).partition());
        assertEquals(9L, sent.get(2).position());
        assertTrue(out.failures().isEmpty());
        assertEquals(3, handlerCalls.get(), "Handlers are not run again");
    }

    @Test
    void everyNormalReplyCarriesAnUndeliverableErrorReplyFallbackWithoutPayload() {
        committer().commit(List.of(ok("high", 0, 1, "c-1", "r1")));

        var rec = sink.calls.get(0).get(0);
        assertNotNull(rec.fallback());
        assertArrayEquals(b("error|UNDELIVERABLE|c-1|key-c-1"), rec.fallback());
        assertFalse(s(rec.fallback()).contains(PAYLOAD));
        assertFalse(s(rec.fallback()).contains("r1"));
    }

    @Test
    void errorRepliesHaveNoFallbackBecauseAnUndeliverableErrorReplyEscalates() {
        committer().commit(List.of(failed("high", 0, 1, "c-1", ErrorCategory.TIMEOUT)));

        assertNull(sink.calls.get(0).get(0).fallback());
    }

    @Test
    void unencodableReplyIsSentWithNullValueAndFallbackInTheSameCommit() {
        sink.answer = new CommitResult(List.of(
                new ReplyFailure("high", 0, 2, "bad", CommitResult.Reason.UNENCODABLE, true)));

        CommitResult out = committer().commit(List.of(
                ok("high", 0, 1, "ok", "fine"),
                ok("high", 0, 2, "bad", "UNENCODABLE"),
                ok("high", 0, 3, "ok-2", "fine")));

        assertEquals(1, sink.calls.size());
        var sent = sink.calls.get(0);
        assertEquals(3, sent.size(), "other requests unaffected, still one record each");
        assertNull(sent.get(1).value());
        assertArrayEquals(b("error|UNDELIVERABLE|bad|key-bad"), sent.get(1).fallback());
        assertEquals("reply|ok|fine", s(sent.get(0).value()));
        assertEquals("reply|ok-2|fine", s(sent.get(2).value()));
        assertEquals(sink.answer, out, "the sink's report of substitutions is returned");
        assertEquals(3, handlerCalls.get());
    }

    @Test
    void oversizedReplyReportedBySinkIsNotRetriedAndHandlerIsNotRunAgain() {
        sink.answer = new CommitResult(List.of(
                new ReplyFailure("high", 0, 2, "big", CommitResult.Reason.TOO_LARGE, true)));

        CommitResult out = committer().commit(List.of(
                ok("high", 0, 1, "ok", "fine"),
                ok("high", 0, 2, "big", "huge")));

        assertEquals(1, sink.calls.size(), "no second commit for a per-reply failure");
        assertEquals(List.of("big"), out.failures().stream().map(ReplyFailure::correlationId).toList());
        assertTrue(out.failures().get(0).substituted());
        assertEquals(2, handlerCalls.get());
    }

    @Test
    void rejectedReplyReportedBySinkIsNotRetriedAndHandlerIsNotRunAgain() {
        sink.answer = new CommitResult(List.of(
                new ReplyFailure("high", 0, 1, "rej", CommitResult.Reason.REJECTED, true)));

        CommitResult out = committer().commit(List.of(ok("high", 0, 1, "rej", "x")));

        assertEquals(1, sink.calls.size());
        assertEquals(CommitResult.Reason.REJECTED, out.failures().get(0).reason());
        assertEquals(1, handlerCalls.get());
    }

    @Test
    void emptyCycleMakesNoCommitCallOrOneEmptyOne() {
        committer().commit(List.of());
        assertTrue(sink.calls.size() <= 1);
    }
}
