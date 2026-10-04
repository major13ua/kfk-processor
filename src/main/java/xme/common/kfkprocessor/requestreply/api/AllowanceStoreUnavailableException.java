package xme.common.kfkprocessor.requestreply.api;

/** The shared Rate Budget store cannot be reached; intake fails closed and the worker pauses. */
public class AllowanceStoreUnavailableException extends RuntimeException {

    public AllowanceStoreUnavailableException(String message) {
        super(message);
    }

    public AllowanceStoreUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
