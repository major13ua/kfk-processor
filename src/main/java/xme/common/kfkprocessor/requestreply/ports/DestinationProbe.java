package xme.common.kfkprocessor.requestreply.ports;

/** Internal port: checks that the reply destination can be written again (no data is committed). */
public interface DestinationProbe {

    /** Returns normally when writable; throws {@link ReplyDestinationFault} (typed) when it still is not. */
    void probe();
}
