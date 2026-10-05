package xme.common.kfkprocessor.requestreply.engine;

/**
 * The worker thread was interrupted (graceful stop) mid-Cycle: the Cycle is abandoned, nothing is committed and the
 * requests stay uncommitted for redelivery. Not a failure: no Error Reply, no destination alert.
 */
public final class CycleInterruptedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CycleInterruptedException(Throwable cause) {
        super("Cycle interrupted by stop", cause);
    }
}
