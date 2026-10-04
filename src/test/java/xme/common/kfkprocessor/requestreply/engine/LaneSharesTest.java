package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LaneSharesTest {

    private static final double MIN = 0.05;
    private static final double EPS = 1e-9;

    private static Map<String, Double> weights(Object... kv) {
        Map<String, Double> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], ((Number) kv[i + 1]).doubleValue());
        }
        return m;
    }

    private static int sum(Map<String, Integer> units) {
        return units.values().stream().mapToInt(Integer::intValue).sum();
    }

    // AC-11
    @Test
    void weights5_3_2_giveSharesHalf_threeTenths_fifth() {
        Map<String, Double> s = LaneShares.effectiveShares(weights("a", 5, "b", 3, "c", 2), MIN);
        assertEquals(0.5, s.get("a"), EPS);
        assertEquals(0.3, s.get("b"), EPS);
        assertEquals(0.2, s.get("c"), EPS);
    }

    @Test
    void effectiveSharesSumToOne() {
        Map<String, Double> s = LaneShares.effectiveShares(weights("a", 5, "b", 3, "c", 2), MIN);
        assertEquals(1.0, s.values().stream().mapToDouble(Double::doubleValue).sum(), EPS);
    }

    @Test
    void splitAllBusyFollowsWeights() {
        Map<String, Integer> u =
                LaneShares.split(weights("a", 5, "b", 3, "c", 2), MIN, Set.of("a", "b", "c"), 100);
        assertEquals(50, u.get("a"));
        assertEquals(30, u.get("b"));
        assertEquals(20, u.get("c"));
    }

    // AC-12
    @Test
    void laneBelowMinimumIsRaisedToFivePercentAndOthersScaledDown() {
        Map<String, Double> s = LaneShares.effectiveShares(weights("big", 99, "tiny", 1), MIN);
        assertEquals(0.05, s.get("tiny"), EPS);
        assertEquals(0.95, s.get("big"), EPS);
    }

    @Test
    void minimumLaneStillGetsUnitsWhenOthersAreHeavy() {
        Map<String, Integer> u =
                LaneShares.split(weights("big", 99, "tiny", 1), MIN, Set.of("big", "tiny"), 100);
        assertEquals(5, u.get("tiny"));
        assertEquals(95, u.get("big"));
    }

    @Test
    void idleLaneShareGoesToBusyLanesProportionally() {
        // a=0.5, c=0.2 busy, b idle: 70 units split 50:20
        Map<String, Integer> u =
                LaneShares.split(weights("a", 5, "b", 3, "c", 2), MIN, Set.of("a", "c"), 70);
        assertEquals(50, u.get("a"));
        assertEquals(0, u.get("b"));
        assertEquals(20, u.get("c"));
    }

    @Test
    void allIdleLanesGetZeroUnits() {
        Map<String, Integer> u =
                LaneShares.split(weights("a", 5, "b", 3, "c", 2), MIN, Set.of(), 100);
        assertEquals(0, sum(u));
        assertEquals(3, u.size());
    }

    @Test
    void singleLaneGetsWholeShareAndWholeDraw() {
        Map<String, Double> s = LaneShares.effectiveShares(weights("only", 7), MIN);
        assertEquals(1.0, s.get("only"), EPS);
        assertEquals(42, LaneShares.split(weights("only", 7), MIN, Set.of("only"), 42).get("only"));
    }

    @Test
    void roundingKeepsExactUnitSum() {
        Map<String, Integer> u =
                LaneShares.split(weights("a", 1, "b", 1, "c", 1), MIN, Set.of("a", "b", "c"), 10);
        assertEquals(10, sum(u));
        u.values().forEach(v -> org.junit.jupiter.api.Assertions.assertTrue(v == 3 || v == 4));
    }

    @Test
    void roundingWithIdleLaneKeepsExactUnitSum() {
        Map<String, Integer> u =
                LaneShares.split(weights("a", 5, "b", 3, "c", 2), MIN, Set.of("a", "c"), 101);
        assertEquals(101, sum(u));
        assertEquals(0, u.get("b"));
    }

    @Test
    void zeroWeightIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> LaneShares.effectiveShares(weights("a", 5, "b", 0), MIN));
    }

    @Test
    void negativeWeightIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> LaneShares.split(weights("a", 5, "b", -1), MIN, Set.of("a", "b"), 10));
    }

    @Test
    void tinyGrantsConvergeToEffectiveSharesWithCarryOver() {
        Map<String, Double> w = weights("a", 1, "b", 60, "c", 39);
        Map<String, Double> credit = new java.util.HashMap<>();
        Map<String, Integer> total = new LinkedHashMap<>();
        for (int i = 0; i < 1000; i++) {
            Map<String, Integer> u = LaneShares.split(w, 0.05, Set.of("a", "b", "c"), 1, credit);
            assertEquals(1, u.values().stream().mapToInt(Integer::intValue).sum());
            u.forEach((k, v) -> total.merge(k, v, Integer::sum));
        }
        // effective shares: a 5%, b 57.5%, c 37.5%
        assertEquals(50, total.get("a"), 1);
        assertEquals(575, total.get("b"), 2);
        assertEquals(375, total.get("c"), 2);
    }

    @Test
    void idleLaneCreditIsDroppedAndUnitsStillSumToDraw() {
        Map<String, Double> w = weights("a", 1, "b", 1);
        Map<String, Double> credit = new java.util.HashMap<>();
        for (int i = 0; i < 100; i++) {
            Map<String, Integer> u = LaneShares.split(w, 0.05, Set.of("a"), 3, credit);
            assertEquals(3, u.get("a"));
            assertEquals(0, u.get("b"));
        }
        assertEquals(false, credit.containsKey("b"));
    }
}
