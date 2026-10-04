package xme.common.kfkprocessor.requestreply.engine;

import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.api.Reply;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;

/** Exactly one outcome per request: a normal {@code reply} or an {@code errorReply}, never both. */
public record HandlerResult<K, RES>(IncomingRequest request, Reply<K, RES> reply, ErrorReply<K> errorReply) {

    public boolean isSuccess() {
        return reply != null;
    }
}
