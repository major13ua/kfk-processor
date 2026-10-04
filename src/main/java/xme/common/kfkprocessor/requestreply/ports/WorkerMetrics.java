package xme.common.kfkprocessor.requestreply.ports;

import java.time.Duration;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;

/** Internal port: observability signals (sad.md §6 flow 8). Tags only lane and category, never payload or keys. */
public interface WorkerMetrics {

    enum State { RUNNING, PAUSED, STALLED }

    void accepted(String lane, long count);

    void consistencyLag(String lane, Duration lag, boolean implausible);

    void state(State state);

    void errorReply(ErrorCategory category);

    void handlerTimeout();

    void commitAttempt();

    void cycleDuration(Duration duration);

    void groupMembershipChange();
}
