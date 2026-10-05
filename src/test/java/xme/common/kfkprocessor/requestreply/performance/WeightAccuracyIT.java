package xme.common.kfkprocessor.requestreply.performance;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;
import static xme.common.kfkprocessor.requestreply.performance.PerfHarness.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import xme.common.kfkprocessor.requestreply.engine.LaneShares;

/** AC-11 and AC-12 (QG-4): three busy lanes at the Rate Budget; per-lane accepted-rate metric against effective share. */
@Testcontainers
class WeightAccuracyIT {

    /** Realistic budget (the per-group throughput target is 2,000/s). */
    private static final long BUDGET = 2000;
    /** Small budget: the Bucket4j burst cap (budget / 20) leaves each Cycle a grant of only a few units. */
    private static final long LOW_BUDGET = 200;
    private static final double TOLERANCE = 0.10; // spec section 6: +-10 percentage points
    private static final double MIN_SHARE = 0.05;
    private static final double MEASUREMENT_SLACK = 0.005; // 0.5 pp

    @Test
    void eachBusyLaneIsWithinTenPointsOfItsEffectiveShare() {
        assertWithinTolerance(new double[] {50, 30, 20}, BUDGET);
    }

    @Test
    void aOnePercentLaneStillGetsItsMinimumShareAndEveryLaneIsWithinTenPointsUnderHeavyNeighbours() {
        assertWithinTolerance(new double[] {1, 60, 39}, BUDGET);
    }

    @Test
    void atALowBudgetEachBusyLaneIsWithinTenPointsAndTheOnePercentLaneIsNotStarved() {
        assertWithinTolerance(new double[] {1, 60, 39}, LOW_BUDGET);
    }

    @Test
    void atALowBudgetEachBusyLaneIsWithinTenPointsOfItsEffectiveShare() {
        assertWithinTolerance(new double[] {50, 30, 20}, LOW_BUDGET);
    }

    /**
     * Spec section 6: every lane within +-10 percentage points of its effective share. AC-12: a lane whose weight is
     * below the minimum share gets at least the minimum share; that is measured over a finite window of accepted
     * counts, so it is checked with {@link #MEASUREMENT_SLACK} (a few units of window-edge and per-worker rounding
     * noise out of thousands; a starved lane would be far below it).
     */
    private void assertWithinTolerance(double[] weights, long budget) {
        Map<String, Double> share = run(weights, budget);
        Map<String, Double> w = new LinkedHashMap<>();
        String[] names = {"lane-a", "lane-b", "lane-c"};
        for (int i = 0; i < 3; i++) {
            w.put(names[i], weights[i]);
        }
        Map<String, Double> effective = LaneShares.effectiveShares(w, MIN_SHARE);
        effective.forEach((lane, expected) -> {
            String why = "accepted share of " + lane + " at budget " + budget + "/s (effective " + expected
                    + "), all shares " + share;
            assertThat(share.get(lane)).as(why).isBetween(expected - TOLERANCE, expected + TOLERANCE);
            if (weights[java.util.Arrays.asList(names).indexOf(lane)] / java.util.Arrays.stream(weights).sum()
                    < MIN_SHARE) {
                assertThat(share.get(lane)).as("minimum share, " + why)
                        .isGreaterThanOrEqualTo(MIN_SHARE - MEASUREMENT_SLACK);
            }
        });
    }

    /** Runs two workers over three busy lanes; returns each lane's share of accepted requests in a steady window. */
    private Map<String, Double> run(double[] weights, long budget) {
        int window = (int) (budget * 10);
        int backlog = (int) Math.max(3000, window * 3 / 4 + 500);
        Group g = Group.fresh();
        List<LaneSpec> lanes = new ArrayList<>();
        String[] names = {"lane-a", "lane-b", "lane-c"};
        for (int i = 0; i < 3; i++) {
            String topic = "req-" + names[i] + "-" + g.suffix();
            createTopic(topic, 2);
            produce(topic, names[i], backlog, k -> "k" + k);
            lanes.add(new LaneSpec(names[i], topic, weights[i]));
        }
        List<Worker> workers = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                workers.add(startWorker(props(g, g.identity(i), budget, 100, lanes), (ctx, req) -> "pong:" + req));
            }
            await("warm-up", WAIT, () -> total(workers) >= window / 5);
            Map<String, Double> from = perLane(workers, names);
            double totalFrom = total(workers);
            await("measurement window", WAIT, () -> total(workers) >= totalFrom + window);
            Map<String, Double> to = perLane(workers, names);
            double sum = 0;
            Map<String, Double> delta = new LinkedHashMap<>();
            for (String n : names) {
                delta.put(n, to.get(n) - from.get(n));
                sum += delta.get(n);
            }
            Map<String, Double> share = new LinkedHashMap<>();
            for (String n : names) {
                share.put(n, delta.get(n) / sum);
            }
            System.out.println("[T17 AC-11/12] budget=" + budget + "/s weights=" + java.util.Arrays.toString(weights) + " accepted=" + delta
                    + " shares=" + share);
            return share;
        } finally {
            workers.forEach(Worker::close);
        }
    }

    private static double total(List<Worker> workers) {
        return sum(workers, "requestreply.accepted", "lane", "lane-a")
                + sum(workers, "requestreply.accepted", "lane", "lane-b")
                + sum(workers, "requestreply.accepted", "lane", "lane-c");
    }

    private static Map<String, Double> perLane(List<Worker> workers, String[] names) {
        Map<String, Double> m = new LinkedHashMap<>();
        for (String n : names) {
            m.put(n, sum(workers, "requestreply.accepted", "lane", n));
        }
        return m;
    }
}
