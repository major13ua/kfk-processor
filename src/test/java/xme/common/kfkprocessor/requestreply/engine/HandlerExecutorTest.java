package xme.common.kfkprocessor.requestreply.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.IdempotencyKey;
import xme.common.kfkprocessor.requestreply.api.RequestContext;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;

class HandlerExecutorTest {

    private static final Duration TIMEOUT = Duration.ofMillis(50);
    private static final Duration LONG_DEADLINE = Duration.ofSeconds(10);
    private static final String SECRET = "SECRET-payload-and-stack";

    private static IncomingRequest req(String lane, long pos, String key, String body) {
        return new IncomingRequest(lane, 3, pos, key, "corr-" + pos, Map.of("h", new byte[] {1}), body.getBytes());
    }

    private static HandlerExecutor<String, String, String> executor(
            RequestReplyHandler<String, String, String> h, Duration timeout) {
        return new HandlerExecutor<>(h, r -> new String(r.payload()), timeout);
    }

    @Test
    void failingHandlerGivesFailureErrorReplyWithCategoryOnlyAndOthersGetNormalReplies() {
        var executor = executor((ctx, in) -> {
            if (in.equals("bad")) {
                throw new IllegalStateException(SECRET);
            }
            return in.toUpperCase();
        }, TIMEOUT);

        var results = executor.executeCycle(List.of(req("a", 1, "k1", "ok"), req("a", 2, "k2", "bad")), LONG_DEADLINE);

        assertEquals(2, results.size());
        assertTrue(results.get(0).isSuccess());
        assertEquals("OK", results.get(0).reply().data());
        assertEquals("corr-1", results.get(0).reply().correlationId());
        assertFalse(results.get(1).isSuccess());
        assertEquals(ErrorCategory.FAILURE, results.get(1).errorReply().category());
        assertEquals("corr-2", results.get(1).errorReply().correlationId());
        assertEquals("k2", results.get(1).errorReply().requestKey());
        assertNull(results.get(1).reply());
        assertFalse(results.toString().contains(SECRET), "handler exception text must never reach results");
    }

    @Test
    void failedHandlerIsInvokedExactlyOnce() {
        var calls = new AtomicInteger();
        var executor = executor((ctx, in) -> {
            calls.incrementAndGet();
            throw new RuntimeException(SECRET);
        }, TIMEOUT);

        var results = executor.executeCycle(List.of(req("a", 1, "k", "x")), LONG_DEADLINE);

        assertEquals(1, results.size());
        assertEquals(ErrorCategory.FAILURE, results.get(0).errorReply().category());
        assertEquals(1, calls.get());
    }

    @Test
    void cancelAwareSlowHandlerGetsTimeoutErrorReplyAndSignalIsSet() throws Exception {
        var observedCancel = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var executor = executor((ctx, in) -> {
            calls.incrementAndGet();
            while (!ctx.cancellation().isCancelled()) {
                Thread.onSpinWait();
            }
            observedCancel.countDown();
            ctx.cancellation().throwIfCancelled();
            return "never";
        }, TIMEOUT);

        var results = executor.executeCycle(List.of(req("a", 1, "k", "x")), LONG_DEADLINE);

        assertEquals(1, results.size());
        assertEquals(ErrorCategory.TIMEOUT, results.get(0).errorReply().category());
        assertTrue(observedCancel.await(5, TimeUnit.SECONDS), "handler must see the cancellation signal");
        assertEquals(1, calls.get());
    }

    @Test
    void cancelIgnoringHandlerStillGetsTimeoutAndItsLateCompletionIsDiscarded() throws Exception {
        var release = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var executor = executor((ctx, in) -> {
            calls.incrementAndGet();
            try {
                release.await();
            } finally {
                finished.countDown();
            }
            return "late";
        }, TIMEOUT);

        var results = executor.executeCycle(List.of(req("a", 1, "k", "x")), LONG_DEADLINE);
        assertEquals(1, results.size());
        assertEquals(ErrorCategory.TIMEOUT, results.get(0).errorReply().category());

        release.countDown();
        assertTrue(finished.await(5, TimeUnit.SECONDS));
        assertEquals(ErrorCategory.TIMEOUT, results.get(0).errorReply().category(), "first outcome stands");
        assertNull(results.get(0).reply());
        assertEquals(1, results.size());
        assertEquals(1, calls.get(), "timed-out handler is never re-run");
    }

    @Test
    void cycleDeadlineEndsUnfinishedRequestsWithTimeoutEvenWhenPerRequestTimeoutIsLonger() throws Exception {
        var hold = new CountDownLatch(1);
        var executor = executor((ctx, in) -> {
            if (in.equals("slow")) {
                hold.await();
            }
            return "done-" + in;
        }, Duration.ofSeconds(60));

        long start = System.nanoTime();
        var results = executor.executeCycle(
                List.of(req("low", 1, "k1", "slow"), req("high", 2, "k2", "fast")), Duration.ofMillis(100));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        try {
            assertTrue(elapsedMs < 5_000, "cycle must not wait past its deadline, took " + elapsedMs + " ms");
            assertEquals(2, results.size());
            assertEquals(ErrorCategory.TIMEOUT, results.get(0).errorReply().category());
            assertTrue(results.get(1).isSuccess());
            assertEquals("done-fast", results.get(1).reply().data());
        } finally {
            hold.countDown();
        }
    }

    @Test
    void deadlineTimeoutSignalsCancellationToUnfinishedHandler() throws Exception {
        var sawCancel = new CountDownLatch(1);
        var executor = executor((ctx, in) -> {
            while (!ctx.cancellation().isCancelled()) {
                Thread.onSpinWait();
            }
            sawCancel.countDown();
            return "x";
        }, Duration.ofSeconds(60));

        var results = executor.executeCycle(List.of(req("a", 1, "k", "x")), Duration.ofMillis(100));

        assertEquals(ErrorCategory.TIMEOUT, results.get(0).errorReply().category());
        assertTrue(sawCancel.await(5, TimeUnit.SECONDS));
    }

    @Test
    void everyRequestGetsExactlyOneResultInInputOrder() {
        var executor = executor((ctx, in) -> {
            if (in.equals("bad")) {
                throw new RuntimeException("x");
            }
            return in;
        }, TIMEOUT);
        var requests = List.of(req("a", 1, "k1", "a"), req("a", 2, "k2", "bad"), req("b", 3, "k3", "c"));

        var results = executor.executeCycle(requests, LONG_DEADLINE);

        assertEquals(3, results.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(requests.get(i), results.get(i).request());
            assertTrue(results.get(i).isSuccess() ^ results.get(i).errorReply() != null);
        }
    }

    @Test
    void handlerReceivesIdempotencyKeyAndContextFields() {
        var seen = new ConcurrentHashMap<String, RequestContext<String>>();
        var executor = executor((ctx, in) -> {
            seen.put(in, ctx);
            return in;
        }, TIMEOUT);

        executor.executeCycle(List.of(req("lane-x", 42, "key-1", "p")), LONG_DEADLINE);

        var ctx = seen.get("p");
        assertEquals("key-1", ctx.requestKey());
        assertEquals("corr-42", ctx.correlationId());
        assertEquals(IdempotencyKey.of("lane-x", 3, 42), ctx.idempotencyKey());
        assertEquals("lane-x", ctx.lane());
        assertEquals(1, ctx.headers().get("h").length);
        assertFalse(ctx.cancellation().isCancelled());
    }

    @Test
    void timeoutErrorReplyCarriesNoHandlerOrPayloadText() {
        var executor = executor((ctx, in) -> {
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                throw new RuntimeException(SECRET);
            }
            return in;
        }, TIMEOUT);

        var results = executor.executeCycle(List.of(req("a", 1, "k", SECRET)), LONG_DEADLINE);

        assertEquals(ErrorCategory.TIMEOUT, results.get(0).errorReply().category());
        assertFalse(results.get(0).errorReply().toString().contains(SECRET));
    }

    // AC-07, AC-14 (A6): an interrupt (graceful stop) abandons the Cycle; undecided slots are not turned into TIMEOUT replies
    @Test
    void interruptDuringTheCycleAbandonsItInsteadOfDecidingTimeouts() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        var executor = executor((ctx, in) -> {
            entered.countDown();
            new CountDownLatch(1).await();
            return "never";
        }, Duration.ofSeconds(30));
        var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var returned = new java.util.concurrent.atomic.AtomicReference<Object>();
        Thread worker = new Thread(() -> {
            try {
                returned.set(executor.executeCycle(List.of(req("a", 1, "k1", "x")), LONG_DEADLINE));
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        worker.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        worker.interrupt();
        worker.join(5_000);

        assertEquals(null, returned.get(), "no results (no TIMEOUT Error Replies) for an interrupted Cycle");
        assertTrue(thrown.get() instanceof CycleInterruptedException, "was " + thrown.get());
    }

    // review r2 D: a graceful stop must not start the Handlers still queued behind a same-key Handler
    @Test
    void interruptSkipsQueuedSameKeyHandlersOfTheAbandonedCycle() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger invoked = new AtomicInteger();
        CountDownLatch firstReturned = new CountDownLatch(1);
        var executor = executor((ctx, in) -> {
            invoked.incrementAndGet();
            if (in.equals("first")) {
                entered.countDown();
                while (!ctx.cancellation().isCancelled()) {
                    Thread.sleep(5);
                }
                firstReturned.countDown();
            }
            return in;
        }, Duration.ofSeconds(30));
        Thread worker = new Thread(() -> {
            try {
                executor.executeCycle(List.of(req("a", 1, "same", "first"), req("a", 2, "same", "second")),
                        LONG_DEADLINE);
            } catch (Throwable ignored) {
                // CycleInterruptedException expected
            }
        });
        worker.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        worker.interrupt();
        worker.join(5_000);
        assertTrue(firstReturned.await(5, TimeUnit.SECONDS));
        Thread.sleep(200); // give a wrongly dispatched second Handler time to start

        assertEquals(1, invoked.get(), "the queued same-key Handler is not started after the stop");
    }

    // review r2 D: the Handler gets an unmodifiable copy of the request headers
    @Test
    void handlerGetsAnUnmodifiableCopyOfTheHeaders() {
        var original = new java.util.LinkedHashMap<String, byte[]>();
        original.put("h", new byte[] {1});
        original.put("nullable", null);
        var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var executor = executor((ctx, in) -> {
            try {
                ctx.headers().put("injected", new byte[] {2});
            } catch (Throwable t) {
                thrown.set(t);
            }
            return in;
        }, TIMEOUT);

        executor.executeCycle(List.of(new IncomingRequest("a", 3, 1, "k", "corr-1", original, "p".getBytes())),
                LONG_DEADLINE);

        assertTrue(thrown.get() instanceof UnsupportedOperationException, "was " + thrown.get());
        assertEquals(2, original.size(), "the engine's own header map is untouched");
        assertFalse(original.containsKey("injected"));
    }
}
