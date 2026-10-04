package xme.common.kfkprocessor.requestreply.engine;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import xme.common.kfkprocessor.requestreply.ports.IncomingRequest;

/**
 * Per-Request-Key FIFO inside a lane: requests sharing a key run one after another in list (arrival)
 * order, distinct keys run in parallel on virtual threads.
 */
public final class KeyedDispatcher {

    /**
     * Starts one virtual-thread chain per distinct request key and returns immediately. Within a chain
     * {@code runner} is invoked sequentially in input order; a runner that throws does not stop its chain.
     * The returned future completes when every request has been passed to the runner and the runner returned.
     */
    public CompletableFuture<Void> dispatch(List<IncomingRequest> requests, Consumer<IncomingRequest> runner) {
        var chains = new java.util.LinkedHashMap<Object, List<IncomingRequest>>();
        for (IncomingRequest request : requests) {
            chains.computeIfAbsent(request.requestKey(), k -> new java.util.ArrayList<>()).add(request);
        }
        var futures = new java.util.ArrayList<CompletableFuture<Void>>(chains.size());
        for (List<IncomingRequest> chain : chains.values()) {
            var future = new CompletableFuture<Void>();
            futures.add(future);
            Thread.ofVirtual().start(() -> {
                try {
                    for (IncomingRequest request : chain) {
                        try {
                            runner.accept(request);
                        } catch (Throwable ignored) {
                            // a failing runner must not stop the chain
                        }
                    }
                } finally {
                    future.complete(null);
                }
            });
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
}
