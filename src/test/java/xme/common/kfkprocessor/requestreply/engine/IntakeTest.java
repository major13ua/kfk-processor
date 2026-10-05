package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.AllowanceStoreUnavailableException;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.PauseReason;
import xme.common.kfkprocessor.requestreply.engine.WorkerState.Status;
import xme.common.kfkprocessor.requestreply.ports.AllowanceStore;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;
import xme.common.kfkprocessor.requestreply.ports.WorkerMetrics;

class IntakeTest {

    private static final int DRAW = 10;
    private static final long MAX_BYTES = 100;
    private static final Duration PROBE = Duration.ofSeconds(5);

    private static final class FakeClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        @Override public Instant instant() { return now; }
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId z) { return this; }
    }

    private static final class FakeStore implements AllowanceStore {
        long available = Long.MAX_VALUE;
        boolean down;
        final List<String> calls = new ArrayList<>();
        final List<Long> requested = new ArrayList<>();
        long returned;
        boolean failGiveBack;

        @Override public long reserve(long units) {
            calls.add("reserve");
            requested.add(units);
            if (down) {
                throw new AllowanceStoreUnavailableException("down");
            }
            long granted = Math.min(units, available);
            available -= granted;
            return granted;
        }

        @Override public void giveBack(long units) {
            calls.add("giveBack");
            if (failGiveBack) {
                throw new IllegalStateException("giveBack failed");
            }
            returned += units;
        }
    }

    private static final class FakeLanes implements RequestLanes {
        /** Requests waiting per lane. */
        final Map<String, List<IncomingRequest>> waiting = new LinkedHashMap<>();
        final List<Map<String, Integer>> fetches = new ArrayList<>();
        final List<String> calls;
        int pauses;
        int resumes;
        int failOnFetch = -1;
        final List<IncomingRequest> released = new ArrayList<>();

        FakeLanes(List<String> calls) { this.calls = calls; }

        @Override public List<IncomingRequest> fetch(Map<String, Integer> quotaByLane) {
            calls.add("fetch");
            if (fetches.size() == failOnFetch) {
                throw new IllegalStateException("fetch failed");
            }
            fetches.add(new LinkedHashMap<>(quotaByLane));
            List<IncomingRequest> out = new ArrayList<>();
            quotaByLane.forEach((lane, q) -> {
                List<IncomingRequest> w = waiting.getOrDefault(lane, new ArrayList<>());
                for (int i = 0; i < q && !w.isEmpty(); i++) {
                    out.add(w.remove(0));
                }
            });
            return out;
        }

        @Override public boolean awaitAvailable() {
            calls.add("await");
            return waiting.values().stream().anyMatch(w -> !w.isEmpty());
        }

        @Override public void release(List<IncomingRequest> requests) { released.addAll(requests); }
        /** Requests whose partition the fake reports revoked since they were fetched (identity). */
        final java.util.Set<IncomingRequest> revoked =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        @Override public boolean revokedSinceFetch(IncomingRequest request) { return revoked.contains(request); }
        @Override public void pause() { pauses++; }
        @Override public void resume() { resumes++; }
    }

    private FakeClock clock;
    private FakeStore store;
    private FakeLanes lanes;
    private WorkerState state;
    private Intake intake;
    private final List<CommitRetry.Alert> alerts = new ArrayList<>();
    private long seq;

    @BeforeEach
    void setUp() {
        clock = new FakeClock();
        store = new FakeStore();
        lanes = new FakeLanes(store.calls);
        WorkerMetrics metrics = (WorkerMetrics) Proxy.newProxyInstance(
                WorkerMetrics.class.getClassLoader(), new Class<?>[] {WorkerMetrics.class}, (p, m, a) -> null);
        state = new WorkerState(clock, Duration.ofSeconds(60), metrics);
        Map<String, Double> weights = new LinkedHashMap<>();
        weights.put("high", 3.0);
        weights.put("low", 1.0);
        intake = new Intake(store, lanes, state, clock, weights, 0.05, MAX_BYTES, DRAW, PROBE, alerts::add);
    }

    private IncomingRequest req(String lane) {
        long n = seq++;
        return new IncomingRequest(lane, 0, n, "key-" + n, "corr-" + n, Map.of(), new byte[10]);
    }

    private void backlog(String lane, int n) {
        List<IncomingRequest> l = lanes.waiting.computeIfAbsent(lane, k -> new ArrayList<>());
        for (int i = 0; i < n; i++) {
            l.add(req(lane));
        }
    }

    private static long count(IntakeResult r, String lane) {
        return r.accepted().stream().filter(x -> x.lane().equals(lane)).count();
    }

    // AC-10: reserve before fetching
    @Test
    void reservesBeforeFetching() {
        backlog("high", 20);
        backlog("low", 20);
        intake.intake();
        assertEquals((long) DRAW, store.requested.get(0));
        assertEquals("reserve", store.calls.stream().filter(c -> !c.equals("await")).findFirst().orElseThrow());
        assertTrue(store.calls.indexOf("fetch") > store.calls.indexOf("reserve"));
    }

    // AC-10: wait for data before taking allowance, so no unit is held through a blocking wait
    @Test
    void awaitsDataBeforeReserving() {
        backlog("high", 20);
        intake.intake();
        assertTrue(store.calls.indexOf("await") < store.calls.indexOf("reserve"));
    }

    // AC-10 / AC-18: nothing waiting: store still probed, units returned at once, nothing fetched
    @Test
    void nothingWaitingProbesStoreReturnsUnitsAndDoesNotFetch() {
        IntakeResult r = intake.intake();
        assertTrue(r.accepted().isEmpty());
        assertTrue(lanes.fetches.isEmpty());
        assertEquals(DRAW, store.returned);
    }

    // AC-10: split by lane share, fetched never above granted
    @Test
    void splitsGrantByLaneShareAndFetchesUpToUnits() {
        backlog("high", 20);
        backlog("low", 20);
        store.available = 8;
        IntakeResult r = intake.intake();
        Map<String, Integer> expected = LaneShares.split(
                Map.of("high", 3.0, "low", 1.0), 0.05, java.util.Set.of("high", "low"), 8);
        assertEquals(expected, lanes.fetches.get(0));
        assertEquals(8, r.accepted().size());
        assertEquals(expected.get("high").longValue(), count(r, "high"));
        assertEquals(0, store.returned);
    }

    // AC-10b: no allowance left, nothing fetched, no reply
    @Test
    void noAllowanceFetchesNothingAndRepliesNothing() {
        backlog("high", 5);
        store.available = 0;
        IntakeResult r = intake.intake();
        assertTrue(lanes.fetches.isEmpty());
        assertTrue(r.accepted().isEmpty());
        assertTrue(r.immediateErrors().isEmpty());
        assertEquals(5, lanes.waiting.get("high").size());
    }

    // AC-10: unused units go back at once (idle lane, small backlog)
    @Test
    void returnsUnusedUnitsWithinTheRound() {
        backlog("high", 2);
        IntakeResult r = intake.intake();
        assertEquals(2, r.accepted().size());
        assertEquals(DRAW - 2, store.returned);
        assertEquals("giveBack", store.calls.get(store.calls.size() - 1));
    }

    // AC-10b: malformed requests answered at once, no allowance spent
    @Test
    void malformedRequestsBecomeFailureReplyAndSpendNothing() {
        lanes.waiting.put("high", new ArrayList<>(List.of(
                new IncomingRequest("high", 0, 100, "k", null, Map.of(), new byte[1]),
                new IncomingRequest("high", 0, 101, null, "c", Map.of(), new byte[1]))));
        IntakeResult r = intake.intake();
        assertTrue(r.accepted().isEmpty());
        assertEquals(2, r.immediateErrors().size());
        r.immediateErrors().forEach(e -> assertEquals(ErrorCategory.FAILURE, e.reply().category()));
        assertEquals("c", r.immediateErrors().get(1).reply().correlationId());
        assertEquals(DRAW, store.returned);
    }

    // AC-10b: oversized requests answered at once, no allowance spent
    @Test
    void oversizedRequestBecomesFailureReplyAndSpendsNothing() {
        IncomingRequest big = new IncomingRequest("high", 0, 1, "k", "c", Map.of(), new byte[(int) MAX_BYTES + 1]);
        lanes.waiting.put("high", new ArrayList<>(List.of(big, req("high"))));
        IntakeResult r = intake.intake();
        assertEquals(1, r.accepted().size());
        assertEquals(1, r.immediateErrors().size());
        assertEquals(ErrorCategory.FAILURE, r.immediateErrors().get(0).reply().category());
        assertEquals("c", r.immediateErrors().get(0).reply().correlationId());
        assertEquals("k", r.immediateErrors().get(0).reply().requestKey());
        assertEquals(DRAW - 1, store.returned);
    }

    // AC-18: store down, paused(limiter) at once, fetch nothing, lanes paused (membership kept)
    @Test
    void storeDownPausesWithLimiterReasonAndFetchesNothing() {
        backlog("high", 5);
        store.down = true;
        IntakeResult r = intake.intake();
        assertTrue(r.paused());
        assertTrue(r.accepted().isEmpty());
        assertTrue(lanes.fetches.isEmpty());
        assertEquals(Status.PAUSED, state.evaluate());
        assertEquals(PauseReason.LIMITER, state.pauseReason());
        assertEquals(1, lanes.pauses);
        assertEquals(5, lanes.waiting.get("high").size());
    }

    // AC-18: the limiter pause raises one Operator alert per outage (SAD 8, public-api)
    @Test
    void limiterPauseAlertsOncePerOutage() {
        backlog("high", 5);
        store.down = true;
        intake.intake();
        clock.advance(PROBE);
        intake.intake();
        assertEquals(1, alerts.size());
        assertEquals(PauseReason.LIMITER, alerts.get(0).reason());
        assertEquals("request_reply.rate_budget_store.unavailable", alerts.get(0).faultId());
        store.down = false;
        clock.advance(PROBE);
        intake.intake();
        store.down = true;
        clock.advance(PROBE);
        intake.intake();
        assertEquals(2, alerts.size());
    }

    // AC-10b: a flood of malformed requests is bounded by the grant within one intake
    @Test
    void malformedFloodIsBoundedByTheGrantPerIntake() {
        List<IncomingRequest> flood = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            flood.add(new IncomingRequest("high", 0, i, "k", null, Map.of(), new byte[1]));
        }
        lanes.waiting.put("high", flood);
        IntakeResult r = intake.intake();
        assertTrue(r.immediateErrors().size() <= DRAW);
        assertEquals(DRAW, store.returned);
    }

    // AC-18: probes until the store is back, resumes within 30 s, budget respected
    @Test
    void probesWhileDownAndResumesWithinThirtySeconds() {
        backlog("high", 20);
        store.down = true;
        intake.intake();
        int reservesAfterFirst = store.requested.size();

        clock.advance(Duration.ofSeconds(2));
        assertTrue(intake.intake().paused());
        assertEquals(reservesAfterFirst, store.requested.size(), "no probe before the probe interval");

        clock.advance(PROBE);
        assertTrue(intake.intake().paused());
        assertTrue(store.requested.size() > reservesAfterFirst, "probes after the interval");
        assertTrue(lanes.fetches.isEmpty());

        store.down = false;
        store.available = 4;
        IntakeResult r = null;
        Duration waited = Duration.ZERO;
        while (waited.compareTo(Duration.ofSeconds(30)) <= 0) {
            clock.advance(Duration.ofSeconds(1));
            waited = waited.plusSeconds(1);
            r = intake.intake();
            if (!r.paused()) {
                break;
            }
        }
        assertTrue(!r.paused(), "resumed within 30 s of the store returning");
        assertNull(state.pauseReason());
        assertEquals(1, lanes.resumes);
        assertEquals(4, r.accepted().size(), "never above what the store granted");
    }

    // AC-05: nothing fetched may be lost when a later sub-round fails; the units go back
    @Test
    void failedLaterFetchReleasesWhatWasFetchedAndReturnsAllUnits() {
        backlog("high", 20);
        lanes.failOnFetch = 1;

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> intake.intake());

        assertEquals(7, lanes.released.size(), "first sub-round's requests handed back");
        assertEquals((long) DRAW, store.returned, "every reserved unit returned");
    }

    @Test
    void failedGiveBackStillHandsOverTheFetchedRequests() {
        backlog("high", 3);
        store.failGiveBack = true;

        IntakeResult r = intake.intake();

        assertEquals(3, r.accepted().size(), "requests already fetched are not dropped");
    }

    // AC-05 (F11/A3): a failed intake hands records back in fetch order, so the buffer front stays contiguous
    // and a later Cycle can never commit a lower offset than an earlier one
    @Test
    void failedIntakeReleasesMixedRecordsInFetchOrder() {
        List<IncomingRequest> fetched = new ArrayList<>();
        fetched.add(new IncomingRequest("high", 0, 10, null, "c10", Map.of(), new byte[1]));
        for (long o = 11; o <= 16; o++) {
            fetched.add(new IncomingRequest("high", 0, o, "k" + o, "c" + o, Map.of(), new byte[1]));
        }
        lanes.waiting.put("high", fetched);
        lanes.failOnFetch = 1;
        try {
            intake.intake();
        } catch (IllegalStateException expected) {
            // second sub-round fails after the first returned malformed p0:10 then accepted p0:11
        }
        List<Long> offsets = lanes.released.stream().map(IncomingRequest::position).toList();
        assertEquals(List.of(10L, 11L, 12L, 13L, 14L, 15L, 16L), offsets, "released in fetch (offset) order, not accepted-then-malformed");
    }

    // review r2 A1 (F12): a rebalance inside the intake polls revokes partitions of requests already fetched; they
    // must not enter the Cycle (their positions are re-read or owned elsewhere) and their units go back
    @Test
    void requestsOfAPartitionRevokedDuringIntakeAreDroppedAndTheirUnitsReturned() {
        backlog("high", 3);
        IncomingRequest malformed = new IncomingRequest("high", 0, seq++, null, "corr-m", Map.of(), new byte[1]);
        lanes.waiting.get("high").add(malformed);
        IncomingRequest stale = lanes.waiting.get("high").get(1);
        lanes.revoked.add(stale);
        lanes.revoked.add(malformed);

        IntakeResult r = intake.intake();

        assertEquals(2, r.accepted().size());
        assertTrue(r.accepted().stream().noneMatch(q -> q == stale), "revoked request dropped");
        assertTrue(r.immediateErrors().isEmpty(), "revoked malformed request dropped too");
        assertEquals(DRAW - 2, store.returned, "units of dropped requests returned");
    }
}
