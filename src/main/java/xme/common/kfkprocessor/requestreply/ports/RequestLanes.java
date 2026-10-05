package xme.common.kfkprocessor.requestreply.ports;

import java.util.List;
import java.util.Map;

/** Internal port: the Priority Lanes this worker consumes (all lanes, stable worker identity). */
public interface RequestLanes {

    /**
     * Fetches at most {@code quotaByLane.get(lane)} requests per lane, without exceeding the quotas.
     * Positions advance only through {@link ReplySink#commit}, never here.
     */
    List<IncomingRequest> fetch(Map<String, Integer> quotaByLane);

    /**
     * Waits briefly (without holding any allowance) until requests are available for {@link #fetch}, so the
     * Rate Budget unit is taken close to the hand-off to a Handler (AC-10). Returns false when nothing arrived.
     * Default: assume available.
     */
    default boolean awaitAvailable() {
        return true;
    }

    /** Stops fetching while staying in the group (pause by limiter or reply destination). */
    void pause();

    void resume();
}
