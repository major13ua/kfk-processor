package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;

class KeyedDispatcherTest {

    private static IncomingRequest req(long pos, String key) {
        return new IncomingRequest("lane", 0, pos, key, "corr-" + pos, Map.of(), new byte[0]);
    }

    @Test
    void sameKeyRunsOneAtATimeInArrivalOrder() throws Exception {
        var order = new CopyOnWriteArrayList<Long>();
        var running = new AtomicInteger();
        var maxRunning = new AtomicInteger();

        var done = new KeyedDispatcher().dispatch(List.of(req(1, "k"), req(2, "k"), req(3, "k")), r -> {
            maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            try {
                Thread.sleep(r.position() == 1 ? 40 : 5); // earlier request is slowest: only a queue keeps order
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            order.add(r.position());
            running.decrementAndGet();
        });
        done.get(5, TimeUnit.SECONDS);

        assertEquals(List.of(1L, 2L, 3L), order);
        assertEquals(1, maxRunning.get(), "same-key requests must never overlap");
    }

    @Test
    void differentKeysRunInParallel() throws Exception {
        var bothStarted = new CountDownLatch(2);
        var parallel = new AtomicInteger();

        var done = new KeyedDispatcher().dispatch(List.of(req(1, "k1"), req(2, "k2")), r -> {
            bothStarted.countDown();
            try {
                if (bothStarted.await(5, TimeUnit.SECONDS)) {
                    parallel.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        done.get(10, TimeUnit.SECONDS);

        assertEquals(2, parallel.get(), "both keys must be running at the same time");
    }

    @Test
    void blockedKeyDoesNotHoldBackAnotherKey() throws Exception {
        var release = new CountDownLatch(1);
        var otherDone = new CountDownLatch(1);

        var done = new KeyedDispatcher().dispatch(List.of(req(1, "slow"), req(2, "slow"), req(3, "fast")), r -> {
            try {
                if ("slow".equals(r.requestKey())) {
                    release.await(5, TimeUnit.SECONDS);
                } else {
                    otherDone.countDown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        try {
            assertTrue(otherDone.await(2, TimeUnit.SECONDS), "fast key must run while the slow key is blocked");
            assertFalse(done.isDone());
        } finally {
            release.countDown();
        }
        done.get(5, TimeUnit.SECONDS);
    }

    @Test
    void interleavedKeysKeepArrivalOrderPerKey() throws Exception {
        Map<String, List<Long>> seen = new ConcurrentHashMap<>();

        var done = new KeyedDispatcher()
                .dispatch(List.of(req(1, "a"), req(2, "b"), req(3, "a"), req(4, "b"), req(5, "a")), r ->
                        seen.computeIfAbsent(String.valueOf(r.requestKey()), k -> new CopyOnWriteArrayList<>()).add(r.position()));
        done.get(5, TimeUnit.SECONDS);

        assertEquals(List.of(1L, 3L, 5L), seen.get("a"));
        assertEquals(List.of(2L, 4L), seen.get("b"));
    }

    @Test
    void failingRunnerDoesNotStopTheNextSameKeyRequest() throws Exception {
        var ran = new CopyOnWriteArrayList<Long>();

        var done = new KeyedDispatcher().dispatch(List.of(req(1, "k"), req(2, "k")), r -> {
            ran.add(r.position());
            if (r.position() == 1) {
                throw new IllegalStateException("predecessor fails");
            }
        });
        done.get(5, TimeUnit.SECONDS);

        assertEquals(List.of(1L, 2L), ran);
    }

    @Test
    void everyRequestIsPassedToTheRunnerExactlyOnce() throws Exception {
        var count = new AtomicInteger();

        var done = new KeyedDispatcher()
                .dispatch(List.of(req(1, "a"), req(2, "a"), req(3, "b"), req(4, "c")), r -> count.incrementAndGet());
        done.get(5, TimeUnit.SECONDS);

        assertEquals(4, count.get());
    }

    // AC-07c / AC-13 (review B11): order is per key within a lane; the same key in two lanes is two chains
    @Test
    void sameKeyInTwoLanesRunsInParallel() throws Exception {
        var bothStarted = new CountDownLatch(2);
        var parallel = new AtomicInteger();
        var high = new IncomingRequest("high", 0, 1, "k", "c-1", Map.of(), new byte[0]);
        var low = new IncomingRequest("low", 0, 1, "k", "c-2", Map.of(), new byte[0]);

        var done = new KeyedDispatcher().dispatch(List.of(high, low), r -> {
            bothStarted.countDown();
            try {
                if (bothStarted.await(2, TimeUnit.SECONDS)) {
                    parallel.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        done.get(10, TimeUnit.SECONDS);

        assertEquals(2, parallel.get(), "a slow request in one lane must not hold the same key in another lane");
    }
}
