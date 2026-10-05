package xme.common.kfkprocessor.requestreply.engine;

import java.util.List;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;

/**
 * Outcome of one intake round. {@code accepted} entered the Cycle and keep their allowance unit;
 * {@code immediateErrors} are malformed or oversized requests answered at once, using no allowance. {@code available} is true when requests were waiting but nothing was
 * accepted (zero grant): the worker is not idle and must keep its group membership alive.
 */
public record IntakeResult(List<IncomingRequest> accepted, List<ImmediateError> immediateErrors, boolean paused,
        boolean available) {

    public IntakeResult(List<IncomingRequest> accepted, List<ImmediateError> immediateErrors, boolean paused) {
        this(accepted, immediateErrors, paused, false);
    }


    /** A request that must be answered with an Error Reply now; the request is kept for position commit. */
    public record ImmediateError(IncomingRequest request, ErrorReply<Object> reply) {}
}
