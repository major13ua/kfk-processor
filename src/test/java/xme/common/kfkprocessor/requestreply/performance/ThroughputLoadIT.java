package xme.common.kfkprocessor.requestreply.performance;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;
import static xme.common.kfkprocessor.requestreply.performance.PerfHarness.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * NFR "Aggregate throughput" (spec section 6), provisional target 2,000 requests/s per group. Slow: run with
 * {@code ./gradlew loadTest}. The uniform-key phase ASSERTS the target (the NFR is aggregate throughput of at least
 * 2,000 requests/s); the fast-Handler and hot-key phases are REPORTED only (a miss prints a MISSED line), the hot key
 * being the documented effect of per-key ordering. Functional invariants (everything handled, per-key order) assert in
 * every phase.
 * Targets come from system properties so a changed provisional number is not a code change:
 * {@code load.target} (default 2000), {@code load.seconds} (default 20), {@code load.workers} (4),
 * {@code load.handler-ms} (simulated Handler work, default 2), {@code load.hot-percent} (default 30).
 */
@Tag("load")
@Testcontainers
class ThroughputLoadIT {

    private static final int TARGET = Integer.getInteger("load.target", 2000);
    private static final int SECONDS = Integer.getInteger("load.seconds", 20);
    private static final int WORKERS = Integer.getInteger("load.workers", 4);
    private static final int HANDLER_MS = Integer.getInteger("load.handler-ms", 2);
    private static final int DRAIN_LIMIT_SECONDS = Integer.getInteger("load.drain-limit-seconds", 180);
    private static final int HOT_PERCENT = Integer.getInteger("load.hot-percent", 30);

    /** What one phase measured. */
    private record Result(String name, int handled, double steadyRate, int peakSecond, double cycleMeanMs,
            double cycleMaxMs, int orderViolations, String perSecond) {
    }

    @Test
    void throughputAgainstTheProvisionalTargetWithFastHandlersUniformAndHotKeys() {
        String phases = System.getProperty("load.phases", "fast,uniform,hot");
        List<Result> results = new ArrayList<>();
        Result fast = null;
        Result uniform = null;
        Result hot = null;
        if (phases.contains("fast")) {
            fast = phase("fast Handler, unique keys", 0, i -> "k" + i);
            print(fast);
            results.add(fast);
        }
        if (phases.contains("uniform")) {
            uniform = phase(HANDLER_MS + " ms Handler, unique keys", HANDLER_MS, i -> "k" + i);
            print(uniform);
            results.add(uniform);
        }
        if (phases.contains("hot")) {
            hot = phase(HANDLER_MS + " ms Handler, " + HOT_PERCENT + "% of requests on one key", HANDLER_MS,
                    i -> i % 100 < HOT_PERCENT ? "hot" : "k" + i);
            print(hot);
            results.add(hot);
        }
        StringBuilder out = new StringBuilder("\n[T17 THROUGHPUT] SUMMARY target " + TARGET + " requests/s per group, "
                + WORKERS + " workers, " + (TARGET * SECONDS) + " requests per phase (provisional, spec section 6)\n");
        for (Result r : results) {
            out.append(String.format("[T17 THROUGHPUT] %-52s steady %.0f/s %s | cycle duration mean %.0f ms, max %.0f ms"
                            + " (head-of-line proxy) | per-key order violations %d%n", r.name(), r.steadyRate(),
                    r.steadyRate() >= TARGET ? "MET" : "MISSED", r.cycleMeanMs(),
                    r.cycleMaxMs(), r.orderViolations()));
        }
        if (uniform != null && hot != null) {
            out.append(String.format("[T17 THROUGHPUT] per-key ordering effect: hot-key phase %.0f/s vs unique-key phase"
                    + " %.0f/s (%.0f%%), cycle max %.0f ms vs %.0f ms%n", hot.steadyRate(), uniform.steadyRate(),
                    100 * hot.steadyRate() / Math.max(1, uniform.steadyRate()), hot.cycleMaxMs(), uniform.cycleMaxMs()));
        }
        System.out.println(out);

        if (uniform != null) {
            assertThat(uniform.steadyRate()).as("aggregate throughput with unique keys: " + uniform.name())
                    .isGreaterThanOrEqualTo(TARGET);
        }
        // functional invariants in every phase
        for (Result r : results) {
            assertThat(r.handled()).as("all requests handled: " + r.name()).isGreaterThanOrEqualTo(TARGET * SECONDS);
            assertThat(r.orderViolations()).as("requests of one key run in arrival order: " + r.name()).isZero();
        }
    }

    /** Evidence for a phase that did not drain: committed lag per partition, worker states, thread of the Cycle loop. */
    private static void diagnose(Group g, String topic, List<Worker> workers) {
        try (var admin = admin()) {
            var committed = admin.listConsumerGroupOffsets(g.group()).partitionsToOffsetAndMetadata().get();
            var ends = admin.listOffsets(committed.keySet().stream().collect(java.util.stream.Collectors.toMap(
                    tp -> tp, tp -> org.apache.kafka.clients.admin.OffsetSpec.latest()))).all().get();
            committed.forEach((tp, om) -> System.out.println("[T17 DIAG] " + tp + " committed " + om.offset() + " end "
                    + ends.get(tp).offset()));
            var d = admin.describeConsumerGroups(List.of(g.group())).describedGroups().get(g.group()).get();
            d.members().forEach(m -> System.out.println("[T17 DIAG] member " + m.groupInstanceId().orElse("?") + " "
                    + m.assignment().topicPartitions()));
        } catch (Exception e) {
            System.out.println("[T17 DIAG] admin failed " + e);
        }
        for (int i = 0; i < workers.size(); i++) {
            Worker w = workers.get(i);
            System.out.println("[T17 DIAG] worker " + i + " state " + w.state() + " accepted "
                    + sum(List.of(w), "requestreply.accepted", "lane", "main") + " commits "
                    + count(w, "requestreply.commit.attempts"));
        }
        Thread.getAllStackTraces().forEach((t, st) -> {
            if (t.getName().equals("request-reply-cycle-loop")) {
                System.out.println("[T17 DIAG] cycle loop stack: " + java.util.Arrays.stream(st).limit(12)
                        .map(StackTraceElement::toString).collect(java.util.stream.Collectors.joining(" <- ")));
            }
        });
    }

    private static void print(Result r) {
        System.out.println(String.format("[T17 THROUGHPUT] RESULT %-52s handled %d, steady %.0f/s (peak second %d) %s | cycle"
                + " duration mean %.0f ms, max %.0f ms | per-key order violations %d%n  accepted per second %s",
                r.name(), r.handled(), r.steadyRate(), r.peakSecond(),
                r.steadyRate() >= TARGET ? "MET" : "MISSED", r.cycleMeanMs(), r.cycleMaxMs(),
                r.orderViolations(), r.perSecond()));
    }

    private Result phase(String name, int handlerMs, java.util.function.IntFunction<String> keyOf) {
        int n = TARGET * SECONDS;
        Group g = Group.fresh();
        String topic = "req-" + g.suffix();
        createTopic(topic, WORKERS * 2);
        List<LaneSpec> lanes = List.of(new LaneSpec("main", topic, 1));
        produce(topic, "ld", n, keyOf);

        Starts starts = new Starts();
        Set<String> distinct = ConcurrentHashMap.newKeySet();
        Map<String, Integer> lastSeq = new ConcurrentHashMap<>();
        AtomicInteger violations = new AtomicInteger();
        List<Worker> workers = new ArrayList<>();
        // budget above the target so the budget is not what limits the measurement
        long budget = (long) (TARGET * 1.5);
        try {
            for (int i = 0; i < WORKERS; i++) {
                workers.add(startWorker(props(g, g.identity(i), budget, 500, lanes), (ctx, req) -> {
                    starts.mark();
                    distinct.add(ctx.correlationId());
                    int seq = Integer.parseInt(req.substring(4));
                    lastSeq.merge(ctx.requestKey(), seq, (old, now) -> {
                        if (now < old) {
                            violations.incrementAndGet();
                        }
                        return Math.max(old, now);
                    });
                    if (handlerMs > 0) {
                        TimeUnit.MILLISECONDS.sleep(handlerMs);
                    }
                    return "ok";
                }));
            }
            long began = System.currentTimeMillis();
            long[] lastLog = {began};
            try {
                await("phase '" + name + "' drained", Duration.ofSeconds(DRAIN_LIMIT_SECONDS), () -> {
                    long now = System.currentTimeMillis();
                    if (now - lastLog[0] >= 15_000) {
                        lastLog[0] = now;
                        System.out.println("[T17 THROUGHPUT] progress '" + name + "': " + distinct.size() + "/" + n
                                + " handled after " + (now - began) / 1000 + " s, error replies "
                                + sum(workers, "requestreply.errorreply", "category", "FAILURE")
                                + " FAILURE, " + sum(workers, "requestreply.errorreply", "category", "TIMEOUT")
                                + " TIMEOUT");
                    }
                    return distinct.size() >= n;
                });
            } catch (AssertionError notDrained) {
                diagnose(g, topic, workers);
                System.out.println("[T17 THROUGHPUT] DID NOT DRAIN within " + DRAIN_LIMIT_SECONDS + " s: '" + name
                        + "' handled " + distinct.size() + "/" + n + " (reported; measured over what was handled)");
            }
            List<Long> times = starts.sorted();
            double cycleMean = 0;
            double cycleMax = 0;
            for (Worker w : workers) {
                var t = w.registry.find("requestreply.cycle.duration").timer();
                if (t != null) {
                    cycleMean = Math.max(cycleMean, t.mean(TimeUnit.MILLISECONDS));
                    cycleMax = Math.max(cycleMax, t.max(TimeUnit.MILLISECONDS));
                }
            }
            return new Result(name, distinct.size(), steadyRate(times), maxInAnySecond(times), cycleMean, cycleMax,
                    violations.get(), perSecond(times));
        } finally {
            workers.forEach(Worker::close);
        }
    }

    /** Requests per second between the 10th and 90th percentile of handled requests (excludes ramp-up and tail). */
    private static double steadyRate(List<Long> sorted) {
        if (sorted.size() < 10) {
            return 0;
        }
        int lo = sorted.size() / 10;
        int hi = sorted.size() * 9 / 10;
        long ms = Math.max(1, sorted.get(hi) - sorted.get(lo));
        return (hi - lo) * 1000.0 / ms;
    }
}
