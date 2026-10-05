package xme.common.kfkprocessor.requestreply.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import xme.common.kfkprocessor.requestreply.api.AllowanceStoreUnavailableException;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.PauseReason;
import xme.common.kfkprocessor.requestreply.ports.AllowanceStore;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;

/** Intake: reserve allowance, split by lane share, fetch, return unused, fail closed (ADR-0003, ADR-0006). */
public final class Intake {

    private static final System.Logger LOG = System.getLogger(Intake.class.getName());

    private final AllowanceStore store;
    private final RequestLanes lanes;
    private final WorkerState state;
    private final Clock clock;
    private final Map<String, Double> laneWeights;
    private final double minLaneShare;
    private final long maxPayloadBytes;
    private final int drawPerRound;
    private final Duration probeInterval;
    private final Map<String, Double> laneCredit = new HashMap<>();
    private boolean paused;
    private Instant lastAttempt;

    public Intake(
            AllowanceStore store,
            RequestLanes lanes,
            WorkerState state,
            Clock clock,
            Map<String, Double> laneWeights,
            double minLaneShare,
            long maxPayloadBytes,
            int drawPerRound,
            Duration probeInterval) {
        this.store = store;
        this.lanes = lanes;
        this.state = state;
        this.clock = clock;
        this.laneWeights = laneWeights;
        this.minLaneShare = minLaneShare;
        this.maxPayloadBytes = maxPayloadBytes;
        this.drawPerRound = drawPerRound;
        this.probeInterval = probeInterval;
    }

    /** One intake round. While paused, probes the store at most once per probe interval. */
    public IntakeResult intake() {
        Instant now = clock.instant();
        if (paused && Duration.between(lastAttempt, now).compareTo(probeInterval) < 0) {
            return paused();
        }
        lastAttempt = now;
        // wait for data before taking allowance: a unit held through a blocking poll would be handed to a
        // Handler late and make the accepted rate in a sliding second exceed the Rate Budget (AC-10)
        boolean available = paused || lanes.awaitAvailable();
        long granted;
        try {
            granted = store.reserve(drawPerRound);
        } catch (AllowanceStoreUnavailableException e) {
            if (!paused) {
                paused = true;
                state.pause(PauseReason.LIMITER);
                lanes.pause();
            }
            return paused();
        }
        if (paused) {
            paused = false;
            state.resume(PauseReason.LIMITER);
            lanes.resume();
        }
        if (granted <= 0) {
            return new IntakeResult(List.of(), List.of(), false);
        }
        if (!available) {
            // the store was probed (fail-closed detection) but there is nothing to accept: return the units
            store.giveBack(granted);
            return new IntakeResult(List.of(), List.of(), false);
        }
        List<IncomingRequest> accepted = new ArrayList<>();
        List<IntakeResult.ImmediateError> errors = new ArrayList<>();
        try {
            fetchRounds(granted, accepted, errors);
        } catch (RuntimeException | Error e) {
            // nothing fetched so far may be lost (its positions are uncommitted): hand it back and return the units
            List<IncomingRequest> fetched = new ArrayList<>(accepted);
            errors.forEach(err -> fetched.add(err.request()));
            try {
                lanes.release(fetched);
                store.giveBack(granted);
            } catch (RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        long unused = granted - accepted.size();
        if (unused > 0) {
            try {
                store.giveBack(unused);
            } catch (RuntimeException e) {
                // the requests are already fetched: handing them over beats losing them; the unreturned units
                // only cost allowance (never exceed the Rate Budget)
                LOG.log(System.Logger.Level.WARNING, "Returning unused allowance failed: " + e.getClass().getName());
            }
        }
        return new IntakeResult(accepted, errors, false);
    }

    private void fetchRounds(long granted, List<IncomingRequest> accepted, List<IntakeResult.ImmediateError> errors) {
        Set<String> busy = laneWeights.keySet();
        while (granted - accepted.size() > 0 && !busy.isEmpty()) {
            Map<String, Integer> quota =
                    LaneShares.split(laneWeights, minLaneShare, busy, (int) (granted - accepted.size()), laneCredit);
            Map<String, Integer> got = new HashMap<>();
            for (IncomingRequest r : lanes.fetch(quota)) {
                got.merge(r.lane(), 1, Integer::sum);
                if (r.requestKey() == null || r.correlationId() == null || r.payload() == null
                        || r.payload().length > maxPayloadBytes) {
                    errors.add(new IntakeResult.ImmediateError(
                            r, ErrorReply.of(ErrorCategory.FAILURE, r.correlationId(), r.requestKey())));
                } else {
                    accepted.add(r);
                }
            }
            // lanes that filled their quota may hold more; the others are idle, their units are redistributed
            Set<String> saturated = new LinkedHashSet<>();
            quota.forEach((lane, q) -> {
                if (q > 0 && got.getOrDefault(lane, 0) >= q) {
                    saturated.add(lane);
                }
            });
            busy = saturated;
        }
    }

    /** Keeps the group membership alive while the worker is paused or holding a Cycle (consumes nothing). */
    public void keepAlive() {
        lanes.keepAlive();
    }

    private static IntakeResult paused() {
        return new IntakeResult(List.of(), List.of(), true);
    }
}
