package xme.common.kfkprocessor.requestreply.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.api.Reply;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;

/**
 * Builds the single commit of a Cycle from Handler results (T11). Every normal reply carries a pre-encoded
 * undeliverable Error Reply as fallback, which the sink sends in the same transaction when the reply cannot be
 * delivered. Handlers are never re-run.
 */
public final class CycleCommitter<K, RES> {

    private final ReplySink sink;
    private final Function<Reply<K, RES>, byte[]> replyEncoder;
    private final Function<ErrorReply<K>, byte[]> errorReplyEncoder;

    public CycleCommitter(ReplySink sink, Function<Reply<K, RES>, byte[]> replyEncoder,
            Function<ErrorReply<K>, byte[]> errorReplyEncoder) {
        this.sink = sink;
        this.replyEncoder = replyEncoder;
        this.errorReplyEncoder = errorReplyEncoder;
    }

    /** One commit call for the whole Cycle; never re-invokes Handlers. Returns the sink's result as is. */
    public CommitResult commit(List<HandlerResult<K, RES>> results) {
        List<ReplyRecord> records = new ArrayList<>(results.size());
        for (HandlerResult<K, RES> result : results) {
            records.add(record(result));
        }
        return sink.commit(records);
    }

    private ReplyRecord record(HandlerResult<K, RES> result) {
        IncomingRequest q = result.request();
        if (!result.isSuccess()) {
            ErrorReply<K> e = result.errorReply();
            return new ReplyRecord(q.lane(), q.partition(), q.position(), e.correlationId(), e.requestKey(),
                    errorReplyEncoder.apply(e), true, null);
        }
        Reply<K, RES> reply = result.reply();
        byte[] fallback = errorReplyEncoder.apply(
                ErrorReply.of(ErrorCategory.UNDELIVERABLE, reply.correlationId(), reply.requestKey()));
        byte[] value;
        try {
            value = replyEncoder.apply(reply);
        } catch (RuntimeException ex) {
            value = null; // the sink reports it as unencodable and sends the fallback
        }
        return new ReplyRecord(q.lane(), q.partition(), q.position(), reply.correlationId(), reply.requestKey(),
                value, false, fallback);
    }
}
