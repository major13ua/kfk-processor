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
import java.util.function.Consumer;
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

    private static final String LIMITER_UNAVAILABLE = "request_reply.rate_budget_store.unavailable";

    private final AllowanceStore store;
    private final RequestLanes lanes;
    private final WorkerState state;
    private final Clock clock;
    private final Map<String, Double> laneWeights;
    private final double minLaneShare;
    private final long maxPayloadBytes;
    private final int drawPerRound;
    private final Duration probeInterval;
    private final Consumer<CommitRetry.Alert> alert;
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
        this(store, lanes, state, clock, laneWeights, minLaneShare, maxPayloadBytes, drawPerRound, probeInterval,
                a -> { });
    }

    public Intake(
            AllowanceStore store,
            RequestLanes lanes,
            WorkerState state,
            Clock clock,
            Map<String, Double> laneWeights,
            double minLaneShare,
            long maxPayloadBytes,
            int drawPerRound,
            Duration probeInterval,
            Consumer<CommitRetry.Alert> alert) {
        this.alert = alert;
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
                alert.accept(new CommitRetry.Alert(PauseReason.LIMITER, LIMITER_UNAVAILABLE));
            }
            return paused();
        }
        if (paused) {
            paused = false;
            state.resume(PauseReason.LIMITER);
            lanes.resume();
        }
        if (granted <= 0) {
            return new IntakeResult(List.of(), List.of(), false, available);
        }
        if (!available) {
            // the store was probed (fail-closed detection) but there is nothing to accept: return the units
            store.giveBack(granted);
            return new IntakeResult(List.of(), List.of(), false);
        }
        List<IncomingRequest> accepted = new ArrayList<>();
        List<IntakeResult.ImmediateError> errors = new ArrayList<>();
        List<IncomingRequest> fetched = new ArrayList<>();
        try {
            fetchRounds(granted, accepted, errors, fetched);
        } catch (RuntimeException | Error e) {
            // nothing fetched so far may be lost (its positions are uncommitted): hand it back and return the units
            // fetched is in poll order: the buffer front must stay contiguous so no later Cycle commits a lower offset
            try {
                lanes.release(fetched);
                store.giveBack(granted);
            } catch (RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        // a rebalance inside the fetch polls may have revoked partitions of requests fetched before it: they are
        // read again from the committed offset (or by the new owner), so they do not enter this Cycle
        accepted.removeIf(lanes::revokedSinceFetch);
        errors.removeIf(e -> lanes.revokedSinceFetch(e.request()));
        fetched.removeIf(lanes::revokedSinceFetch);
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
        return new IntakeResult(accepted, errors, false, false, fetched);
    }

    private void fetchRounds(long granted, List<IncomingRequest> accepted, List<IntakeResult.ImmediateError> errors,
            List<IncomingRequest> fetched) {
        Set<String> busy = laneWeights.keySet();
        while (granted - accepted.size() - errors.size() > 0 && !busy.isEmpty()) {
            Map<String, Integer> quota =
                    LaneShares.split(laneWeights, minLaneShare, busy, (int) (granted - accepted.size() - errors.size()), laneCredit);
            Map<String, Integer> got = new HashMap<>();
            for (IncomingRequest r : lanes.fetch(quota)) {
                got.merge(r.lane(), 1, Integer::sum);
                fetched.add(r);
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

    /**
     * A Cycle failed after intake: hands every kept request back in poll order and returns the allowance units of the
     * accepted ones (immediate errors used none), like the fetch failure path.
     */
    public void abandon(IntakeResult in) {
        abandon(in, false);
    }

    /**
     * As {@link #abandon(IntakeResult)}; when the Handlers already ran ({@code handlersRan}) the allowance units were
     * spent and stay spent, only the requests are handed back.
     */
    public void abandon(IntakeResult in, boolean handlersRan) {
        RuntimeException failure = null;
        try {
            lanes.release(in.fetched());
        } catch (RuntimeException e) {
            failure = e;
        }
        try {
            if (!handlersRan) {
                store.giveBack(in.accepted().size());
            }
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** See {@link RequestLanes#revokedSinceFetch}. */
    public boolean revokedSinceFetch(IncomingRequest request) {
        return lanes.revokedSinceFetch(request);
    }

    /** See {@link RequestLanes#committedAfterHold}. */
    public void committedAfterHold(List<IncomingRequest> requests) {
        lanes.committedAfterHold(requests);
    }

    private static IntakeResult paused() {
        return new IntakeResult(List.of(), List.of(), true);
    }
}
