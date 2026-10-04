package xme.common.kfkprocessor.requestreply.api;

/** Idempotency Key = lane:partition:position of the request; stable across re-execution, identifies the attempt. */
public final class IdempotencyKey {

    private IdempotencyKey() {
    }

    public static String of(String lane, int partition, long position) {
        return lane + ":" + partition + ":" + position;
    }
}
