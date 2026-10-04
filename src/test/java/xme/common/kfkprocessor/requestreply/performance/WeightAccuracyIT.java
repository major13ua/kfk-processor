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
@Testcontainers(disabledWithoutDocker = true)
class WeightAccuracyIT {

    /** Realistic budget (the per-group throughput target is 2,000/s). */
    private static final long BUDGET = 2000;
    /** Small budget: the Bucket4j burst cap (budget / 20) leaves each Cycle a grant of only a few units. */
    private static final long LOW_BUDGET = 200;
    private static final double TOLERANCE = 0.10; // spec section 6: +-10 percentage points
    private static final double MIN_SHARE = 0.05;

    @Test
    void eachBusyLaneIsWithinTenPointsOfItsEffectiveShare() {
        Map<String, Double> share = run(new double[] {50, 30, 20}, BUDGET);
        Map<String, Double> effective = LaneShares.effectiveShares(
                Map.of("lane-a", 50.0, "lane-b", 30.0, "lane-c", 20.0), MIN_SHARE);
        effective.forEach((lane, expected) -> assertThat(share.get(lane))
                .as("accepted share of " + lane + " (effective " + expected + "), all shares " + share)
                .isBetween(expected - TOLERANCE, expected + TOLERANCE));
    }

    @Test
    void aOnePercentLaneStillGetsItsMinimumShareUnderHeavyNeighbours() {
        Map<String, Double> share = run(new double[] {1, 60, 39}, BUDGET);
        assertThat(share.get("lane-a")).as("accepted share of the 1% lane, all shares " + share)
                .isGreaterThanOrEqualTo(MIN_SHARE);
        Map<String, Double> effective = LaneShares.effectiveShares(
                Map.of("lane-a", 1.0, "lane-b", 60.0, "lane-c", 39.0), MIN_SHARE);
        effective.forEach((lane, expected) -> assertThat(share.get(lane))
                .as("accepted share of " + lane + " (effective " + expected + "), all shares " + share)
                .isBetween(expected - TOLERANCE, expected + TOLERANCE));
    }

    @Test
    void atALowBudgetEachBusyLaneIsWithinTenPointsAndTheOnePercentLaneIsNotStarved() {
        Map<String, Double> share = run(new double[] {1, 60, 39}, LOW_BUDGET);
        assertThat(share.get("lane-a")).as("accepted share of the 1% lane at budget " + LOW_BUDGET + "/s, all shares "
                + share).isGreaterThanOrEqualTo(MIN_SHARE);
    }

    @Test
    void atALowBudgetEachBusyLaneIsWithinTenPointsOfItsEffectiveShare() {
        Map<String, Double> share = run(new double[] {50, 30, 20}, LOW_BUDGET);
        Map<String, Double> effective = LaneShares.effectiveShares(
                Map.of("lane-a", 50.0, "lane-b", 30.0, "lane-c", 20.0), MIN_SHARE);
        effective.forEach((lane, expected) -> assertThat(share.get(lane))
                .as("accepted share of " + lane + " at budget " + LOW_BUDGET + "/s (effective " + expected
                        + "), all shares " + share)
                .isBetween(expected - TOLERANCE, expected + TOLERANCE));
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
