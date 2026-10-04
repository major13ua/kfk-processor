package xme.common.kfkprocessor.requestreply.api;

import java.util.concurrent.CancellationException;

/** Cooperative cancellation, set once the per-request timeout elapsed. */
public interface CancellationSignal {

    boolean isCancelled();

    void throwIfCancelled() throws CancellationException;
}
