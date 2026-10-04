package xme.common.kfkprocessor.requestreply.engine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import xme.common.kfkprocessor.requestreply.api.CancellationSignal;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;
import xme.common.kfkprocessor.requestreply.api.IdempotencyKey;
import xme.common.kfkprocessor.requestreply.api.Reply;
import xme.common.kfkprocessor.requestreply.api.RequestContext;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;

/** Runs Handler calls on virtual threads under a per-request timeout and the Cycle deadline. */
public final class HandlerExecutor<K, REQ, RES> {

    private final RequestReplyHandler<K, REQ, RES> handler;
    private final Function<IncomingRequest, REQ> decoder;
    private final Duration requestTimeout;

    public HandlerExecutor(
            RequestReplyHandler<K, REQ, RES> handler,
            Function<IncomingRequest, REQ> decoder,
            Duration requestTimeout) {
        this.handler = handler;
        this.decoder = decoder;
        this.requestTimeout = requestTimeout;
    }

    /** Dispatches all requests in parallel; returns one result per request, in input order, within cycleDeadline. */
    public List<HandlerResult<K, RES>> executeCycle(List<IncomingRequest> requests, Duration cycleDeadline) {
        long budgetNanos = Math.min(requestTimeout.toNanos(), cycleDeadline.toNanos());
        long dispatchedAt = System.nanoTime();
        var slots = new ArrayList<Slot>(requests.size());
        var done = new CountDownLatch(requests.size());
        for (IncomingRequest request : requests) {
            var slot = new Slot(request);
            slots.add(slot);
            Thread.ofVirtual().start(() -> run(slot, done));
        }
        long remaining = budgetNanos - (System.nanoTime() - dispatchedAt);
        try {
            done.await(Math.max(remaining, 0), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        var results = new ArrayList<HandlerResult<K, RES>>(slots.size());
        for (Slot slot : slots) {
            if (slot.outcome.compareAndSet(null, errorResult(slot.request, ErrorCategory.TIMEOUT))) {
                slot.cancelled.set(true);
            }
            results.add(slot.outcome.get());
        }
        return results;
    }

    private void run(Slot slot, CountDownLatch done) {
        try {
            IncomingRequest r = slot.request;
            @SuppressWarnings("unchecked")
            K key = (K) r.requestKey();
            CancellationSignal signal = new CancellationSignal() {
                @Override
                public boolean isCancelled() {
                    return slot.cancelled.get();
                }

                @Override
                public void throwIfCancelled() {
                    if (slot.cancelled.get()) {
                        throw new CancellationException();
                    }
                }
            };
            var ctx = new RequestContext<>(
                    key,
                    r.correlationId(),
                    IdempotencyKey.of(r.lane(), r.partition(), r.position()),
                    r.lane(),
                    r.headers(),
                    signal);
            RES data = handler.handle(ctx, decoder.apply(r));
            slot.outcome.compareAndSet(
                    null, new HandlerResult<>(r, new Reply<>(r.correlationId(), key, data), null));
        } catch (Throwable t) {
            slot.outcome.compareAndSet(null, errorResult(slot.request, ErrorCategory.FAILURE));
        } finally {
            done.countDown();
        }
    }

    @SuppressWarnings("unchecked")
    private HandlerResult<K, RES> errorResult(IncomingRequest r, ErrorCategory category) {
        return new HandlerResult<>(r, null, ErrorReply.of(category, r.correlationId(), (K) r.requestKey()));
    }

    private final class Slot {
        final IncomingRequest request;
        final AtomicReference<HandlerResult<K, RES>> outcome = new AtomicReference<>();
        final AtomicBoolean cancelled = new AtomicBoolean();

        Slot(IncomingRequest request) {
            this.request = request;
        }
    }
}
