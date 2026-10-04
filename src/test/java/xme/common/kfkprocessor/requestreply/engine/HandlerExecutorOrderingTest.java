package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;

/** AC-07c: per-Request-Key ordering inside a lane, through HandlerExecutor.executeCycle. */
class HandlerExecutorOrderingTest {

    private static final Duration LONG = Duration.ofSeconds(10);

    private static IncomingRequest req(long pos, String key, String body) {
        return new IncomingRequest("lane", 0, pos, key, "corr-" + pos, Map.of(), body.getBytes());
    }

    private static HandlerExecutor<String, String, String> executor(
            RequestReplyHandler<String, String, String> h, Duration timeout) {
        return new HandlerExecutor<>(h, r -> new String(r.payload()), timeout);
    }

    @Test
    void sameKeyRequestsRunSequentiallyInArrivalOrder() {
        var order = new CopyOnWriteArrayList<String>();
        var running = new AtomicInteger();
        var overlapped = new AtomicBoolean();
        var executor = executor((ctx, in) -> {
            if (running.incrementAndGet() > 1) {
                overlapped.set(true);
            }
            Thread.sleep(in.equals("first") ? 40 : 5);
            order.add(in);
            running.decrementAndGet();
            return in;
        }, Duration.ofSeconds(5));

        var results = executor.executeCycle(
                List.of(req(1, "k", "first"), req(2, "k", "second"), req(3, "k", "third")), LONG);

        assertEquals(List.of("first", "second", "third"), order);
        assertEquals(false, overlapped.get(), "same-key handlers must not overlap");
        assertEquals(3, results.size());
        results.forEach(r -> assertTrue(r.isSuccess()));
    }

    @Test
    void differentKeysRunInParallel() {
        var bothStarted = new CountDownLatch(2);
        var parallel = new AtomicInteger();
        var executor = executor((ctx, in) -> {
            bothStarted.countDown();
            if (bothStarted.await(5, TimeUnit.SECONDS)) {
                parallel.incrementAndGet();
            }
            return in;
        }, Duration.ofSeconds(10));

        var results = executor.executeCycle(List.of(req(1, "k1", "a"), req(2, "k2", "b")), LONG);

        assertEquals(2, parallel.get());
        results.forEach(r -> assertTrue(r.isSuccess()));
    }

    @Test
    void queuedRequestTimeoutStartsAtDispatchNotArrival() {
        var firstFinished = new AtomicBoolean();
        var secondSawFirstFinished = new AtomicBoolean();
        // each handler takes ~250 ms, per-request timeout 400 ms: the second would exceed it if timed from arrival
        var executor = executor((ctx, in) -> {
            if (in.equals("second")) {
                secondSawFirstFinished.set(firstFinished.get());
            }
            Thread.sleep(250);
            if (in.equals("first")) {
                firstFinished.set(true);
            }
            return in;
        }, Duration.ofMillis(400));

        var results = executor.executeCycle(List.of(req(1, "k", "first"), req(2, "k", "second")), LONG);

        assertTrue(secondSawFirstFinished.get(), "second must be dispatched only after the first finished");
        assertEquals(2, results.size());
        assertTrue(results.get(0).isSuccess(), "first: " + results.get(0));
        assertTrue(results.get(1).isSuccess(), "queued request must get its own full timeout: " + results.get(1));
    }

    @Test
    void queuedRequestCutByCycleDeadlineGetsTimeoutWithoutBeingDispatched() {
        var hold = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var executor = executor((ctx, in) -> {
            calls.incrementAndGet();
            if (in.equals("first")) {
                hold.await(); // predecessor consumes the whole Cycle deadline, ignores cancellation
            }
            return in;
        }, Duration.ofSeconds(60));

        List<HandlerResult<String, String>> results;
        try {
            results = executor.executeCycle(
                    List.of(req(1, "k", "first"), req(2, "k", "second"), req(3, "other", "free")),
                    Duration.ofMillis(150));
        } finally {
            hold.countDown();
        }

        assertEquals(3, results.size());
        assertEquals(ErrorCategory.TIMEOUT, results.get(0).errorReply().category());
        assertEquals(ErrorCategory.TIMEOUT, results.get(1).errorReply().category());
        assertEquals("corr-2", results.get(1).errorReply().correlationId());
        assertTrue(results.get(2).isSuccess(), "different key is not affected by the blocked key");
        // let any wrongly dispatched queued request surface before counting
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(2, calls.get(), "queued request cut by the deadline must not be dispatched");
    }

    @Test
    void failingPredecessorDoesNotStopNextSameKeyRequest() {
        var executor = executor((ctx, in) -> {
            if (in.equals("bad")) {
                throw new IllegalStateException("boom");
            }
            return in;
        }, Duration.ofSeconds(5));

        var results = executor.executeCycle(List.of(req(1, "k", "bad"), req(2, "k", "good")), LONG);

        assertEquals(ErrorCategory.FAILURE, results.get(0).errorReply().category());
        assertTrue(results.get(1).isSuccess());
        assertEquals("good", results.get(1).reply().data());
    }

    @Test
    void oneResultPerRequestInInputOrderWithInterleavedKeys() {
        var executor = executor((ctx, in) -> in, Duration.ofSeconds(5));
        var requests = List.of(req(1, "a", "x1"), req(2, "b", "y1"), req(3, "a", "x2"), req(4, "b", "y2"));

        var results = executor.executeCycle(requests, LONG);

        assertEquals(4, results.size());
        for (int i = 0; i < 4; i++) {
            assertEquals(requests.get(i), results.get(i).request());
            assertTrue(results.get(i).isSuccess());
        }
    }
}
