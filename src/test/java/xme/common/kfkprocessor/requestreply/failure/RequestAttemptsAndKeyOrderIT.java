package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

/** AC-05 (a new attempt is a new request) and AC-07c (per-key order within a lane) on the real platform. */
@Testcontainers
class RequestAttemptsAndKeyOrderIT {

    // AC-05: a Requester that sends the same request again (same correlation id) gets a reply for each attempt
    @Test
    void aNewAttemptByTheRequesterIsTreatedAsANewRequest() {
        Ids ids = Ids.fresh();
        HandlerLog handled = new HandlerLog();
        produce(ids.requests(), List.of("attempt-1"));

        try (Worker w = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    return "pong:" + req;
                })) {
            awaitReplies(ids.replies(), 1, WAIT, Duration.ofMillis(500));
            produce(ids.requests(), List.of("attempt-1")); // the Requester retries: a second attempt

            List<Rep> replies = awaitReplies(ids.replies(), 2, WAIT, Duration.ofSeconds(3));

            assertThat(replies).as("one committed reply per attempt").hasSize(2)
                    .allSatisfy(r -> {
                        assertThat(r.correlationId()).isEqualTo("attempt-1");
                        assertThat(r.type()).isEqualTo("reply");
                        assertThat(r.body()).isEqualTo("pong:req-attempt-1");
                    });
            assertThat(handled.calls).as("the Handler ran once per attempt").hasSize(2);
            assertThat(handled.calls.get(0).idempotencyKey())
                    .as("each attempt is its own request, with its own Idempotency Key")
                    .isNotEqualTo(handled.calls.get(1).idempotencyKey());
        }
    }

    // AC-07c: same Request Key runs one after another in arrival order within the lane, other keys in parallel
    @Test
    void orderingPerKeyHoldsAcrossAFullCycleOnTheRealPlatform() {
        Ids ids = Ids.fresh();
        int perKey = 8;
        List<String> keys = List.of("k-a", "k-b", "k-c");
        List<String> requested = correlationIds("ord", perKey * keys.size());
        Map<String, String> keyOf = new LinkedHashMap<>();
        for (int i = 0; i < requested.size(); i++) {
            keyOf.put(requested.get(i), keys.get(i % keys.size()));
        }
        produce(ids.requests(), requested, keyOf::get);
        HandlerLog handled = new HandlerLog();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        Map<String, AtomicInteger> runningPerKey = new java.util.concurrent.ConcurrentHashMap<>();
        AtomicInteger sameKeyOverlap = new AtomicInteger();

        try (Worker w = startWorker(workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20)),
                (ctx, req) -> {
                    handled.record(ctx.correlationId(), ctx.idempotencyKey());
                    maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
                    if (runningPerKey.computeIfAbsent(ctx.requestKey(), k -> new AtomicInteger()).incrementAndGet() > 1) {
                        sameKeyOverlap.incrementAndGet();
                    }
                    try {
                        Thread.sleep(50);
                    } finally {
                        runningPerKey.get(ctx.requestKey()).decrementAndGet();
                        running.decrementAndGet();
                    }
                    return "pong:" + req;
                })) {
            List<Rep> replies = awaitReplies(ids.replies(), requested.size(), WAIT, Duration.ofSeconds(2));

            assertReconciled(requested, replies);
            assertThat(sameKeyOverlap.get()).as("no two Handlers of one Request Key ran at the same time").isZero();
            assertThat(maxRunning.get()).as("different Request Keys ran in parallel").isGreaterThan(1);
            for (String key : keys) {
                List<String> arrival = requested.stream().filter(c -> keyOf.get(c).equals(key)).toList();
                List<String> started = new ArrayList<>();
                handled.calls.forEach(c -> {
                    if (keyOf.get(c.correlationId()).equals(key)) {
                        started.add(c.correlationId());
                    }
                });
                List<String> replied = replies.stream().filter(r -> key.equals(r.requestKey()))
                        .map(Rep::correlationId).toList();
                assertThat(started).as("Handler start order of " + key).containsExactlyElementsOf(arrival);
                assertThat(replied).as("committed reply order of " + key).containsExactlyElementsOf(arrival);
            }
        }
    }
}
