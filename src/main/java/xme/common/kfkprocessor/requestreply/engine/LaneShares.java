package xme.common.kfkprocessor.requestreply.engine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Pure calculator of effective Priority Lane shares and of weighted splits of an allowance draw. */
public final class LaneShares {

    private static final double EPS = 1e-9;

    private LaneShares() {}

    /**
     * Effective share per lane, summing to 1. Lanes below the minimum are raised to it and the others
     * scaled proportionally. The minimum is clamped to 1/lanes when lanes * minShare would exceed 1.
     *
     * @throws IllegalArgumentException if any weight is not a finite number above 0
     */
    public static Map<String, Double> effectiveShares(Map<String, Double> weights, double minShare) {
        validate(weights);
        Map<String, Double> shares = new LinkedHashMap<>();
        if (weights.isEmpty()) {
            return shares;
        }
        double min = Math.max(0.0, Math.min(minShare, 1.0 / weights.size()));
        Set<String> raised = new HashSet<>();
        boolean changed = true;
        while (changed) {
            changed = false;
            double free = 0;
            for (Map.Entry<String, Double> e : weights.entrySet()) {
                if (!raised.contains(e.getKey())) {
                    free += e.getValue();
                }
            }
            double room = 1.0 - raised.size() * min;
            for (Map.Entry<String, Double> e : weights.entrySet()) {
                if (raised.contains(e.getKey())) {
                    continue;
                }
                if (e.getValue() / free * room < min - EPS) {
                    raised.add(e.getKey());
                    changed = true;
                }
            }
            if (!changed) {
                shares.clear();
                for (Map.Entry<String, Double> e : weights.entrySet()) {
                    shares.put(e.getKey(), raised.contains(e.getKey()) ? min : e.getValue() / free * room);
                }
            }
        }
        return shares;
    }

    /**
     * Splits {@code draw} units across busy lanes by effective share, re-normalised over busy lanes
     * (largest remainder). Idle lanes get 0; the units sum to {@code draw} unless no lane is busy.
     */
    public static Map<String, Integer> split(
            Map<String, Double> weights, double minShare, Set<String> busyLanes, int draw) {
        Map<String, Double> shares = effectiveShares(weights, minShare);
        Map<String, Integer> units = new LinkedHashMap<>();
        List<String> busy = new ArrayList<>();
        double busyTotal = 0;
        for (Map.Entry<String, Double> e : shares.entrySet()) {
            units.put(e.getKey(), 0);
            if (busyLanes.contains(e.getKey())) {
                busy.add(e.getKey());
                busyTotal += e.getValue();
            }
        }
        if (busy.isEmpty() || draw <= 0) {
            return units;
        }
        double[] remainder = new double[busy.size()];
        int assigned = 0;
        for (int i = 0; i < busy.size(); i++) {
            double exact = shares.get(busy.get(i)) / busyTotal * draw;
            int base = (int) Math.floor(exact + EPS);
            units.put(busy.get(i), base);
            remainder[i] = exact - base;
            assigned += base;
        }
        for (int left = draw - assigned; left > 0; left--) {
            int best = 0;
            for (int i = 1; i < remainder.length; i++) {
                if (remainder[i] > remainder[best]) {
                    best = i;
                }
            }
            units.merge(busy.get(best), 1, Integer::sum);
            remainder[best] = -1;
            if (left > 1 && allUsed(remainder)) {
                java.util.Arrays.fill(remainder, 0);
            }
        }
        return units;
    }

    private static boolean allUsed(double[] remainder) {
        for (double r : remainder) {
            if (r >= 0) {
                return false;
            }
        }
        return true;
    }

    private static void validate(Map<String, Double> weights) {
        for (Map.Entry<String, Double> e : weights.entrySet()) {
            Double w = e.getValue();
            if (w == null || !Double.isFinite(w) || w <= 0) {
                throw new IllegalArgumentException("Priority Weight must be above 0 for lane " + e.getKey());
            }
        }
    }
}
