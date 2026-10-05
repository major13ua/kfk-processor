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

    /**
     * Fault id {@code request_reply.reply_destination.fenced}: a newer instance owns the transactional id. The
     * producer is never re-created (that would fence the live instance in turn), so the worker stays paused
     * until it is stopped.
     */
    public static final class Fenced extends ReplyDestinationFault {
        public Fenced(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** The reply destination name is invalid: no reply can ever be written, the worker must not start. */
    public static final class Invalid extends ReplyDestinationFault {
        public Invalid(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
