package xme.common.kfkprocessor.requestreply.engine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
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

    private static final long POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

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

    /**
     * Dispatches requests per Request Key (parallel across keys, FIFO within a key); returns one result per
     * request, in input order, within cycleDeadline. If the calling thread is interrupted (graceful stop) the Cycle
     * is abandoned with {@link CycleInterruptedException}: undecided requests get no result.
     */
    public List<HandlerResult<K, RES>> executeCycle(List<IncomingRequest> requests, Duration cycleDeadline) {
        long cycleEnd = System.nanoTime() + cycleDeadline.toNanos();
        var slots = new ArrayList<Slot>(requests.size());
        var bySlot = new IdentityHashMap<IncomingRequest, Slot>();
        var decided = new CountDownLatch(requests.size());
        for (IncomingRequest request : requests) {
            var slot = new Slot(request, decided);
            slots.add(slot);
            bySlot.put(request, slot);
        }
        new KeyedDispatcher().dispatch(requests, r -> run(bySlot.get(r), cycleEnd));
        try {
            while (decided.getCount() > 0) {
                long now = System.nanoTime();
                long untilCycleEnd = cycleEnd - now;
                if (untilCycleEnd <= 0) {
                    break;
                }
                for (Slot slot : slots) {
                    if (now - slot.deadline >= 0) {
                        slot.decide(errorResult(slot.request, ErrorCategory.TIMEOUT), true);
                    }
                }
                decided.await(Math.min(untilCycleEnd, POLL_NANOS), TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            for (Slot slot : slots) {
                slot.cancelled.set(true); // stop the Handlers; the undecided requests stay uncommitted
            }
            throw new CycleInterruptedException(e);
        }
        var results = new ArrayList<HandlerResult<K, RES>>(slots.size());
        for (Slot slot : slots) {
            slot.decide(errorResult(slot.request, ErrorCategory.TIMEOUT), true);
            results.add(slot.outcome.get());
        }
        return results;
    }

    private void run(Slot slot, long cycleEnd) {
        if (slot.outcome.get() != null || slot.cancelled.get()) {
            return; // already decided (cycle deadline) or the Cycle was abandoned (graceful stop): never dispatch
        }
        slot.deadline = Math.min(System.nanoTime() + requestTimeout.toNanos(), cycleEnd);
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
                    Collections.unmodifiableMap(new LinkedHashMap<>(r.headers())), // a copy: null values allowed
                    signal);
            RES data = handler.handle(ctx, decoder.apply(r));
            slot.decide(new HandlerResult<>(r, new Reply<>(r.correlationId(), key, data), null), false);
        } catch (Throwable t) {
            slot.decide(errorResult(slot.request, ErrorCategory.FAILURE), false);
        }
    }

    @SuppressWarnings("unchecked")
    private HandlerResult<K, RES> errorResult(IncomingRequest r, ErrorCategory category) {
        return new HandlerResult<>(r, null, ErrorReply.of(category, r.correlationId(), (K) r.requestKey()));
    }

    private final class Slot {
        final IncomingRequest request;
        final CountDownLatch decided;
        final AtomicReference<HandlerResult<K, RES>> outcome = new AtomicReference<>();
        final AtomicBoolean cancelled = new AtomicBoolean();
        /** Absolute nanoTime deadline; unset (never expires) until the request is dispatched. */
        volatile long deadline = System.nanoTime() + Long.MAX_VALUE / 2;

        Slot(IncomingRequest request, CountDownLatch decided) {
            this.request = request;
            this.decided = decided;
        }

        void decide(HandlerResult<K, RES> result, boolean cancel) {
            if (outcome.compareAndSet(null, result)) {
                if (cancel) {
                    cancelled.set(true);
                }
                decided.countDown();
            }
        }
    }
}
