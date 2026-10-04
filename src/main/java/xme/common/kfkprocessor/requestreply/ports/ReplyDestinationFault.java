package xme.common.kfkprocessor.requestreply.ports;

/** The reply destination cannot be written; nothing was committed. Subtypes are distinct for the pause/alert logic. */
public abstract class ReplyDestinationFault extends RuntimeException {

    protected ReplyDestinationFault(String message, Throwable cause) {
        super(message, cause);
    }

    /** Fault id {@code request_reply.reply_destination.permission_denied}. */
    public static final class PermissionDenied extends ReplyDestinationFault {
        public PermissionDenied(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Fault id {@code request_reply.reply_destination.unavailable}. */
    public static final class Unavailable extends ReplyDestinationFault {
        public Unavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
