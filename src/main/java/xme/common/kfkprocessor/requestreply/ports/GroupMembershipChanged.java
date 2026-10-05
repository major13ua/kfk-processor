package xme.common.kfkprocessor.requestreply.ports;

/**
 * The consumer group rejected the Cycle's offsets (rebalance, fenced static member): nothing was committed and the
 * reply destination is healthy. Not a {@link ReplyDestinationFault}: the Cycle is re-committed without its revoked
 * requests, with no pause and no destination alert.
 */
public class GroupMembershipChanged extends RuntimeException {
    public GroupMembershipChanged(String message, Throwable cause) {
        super(message, cause);
    }
}
