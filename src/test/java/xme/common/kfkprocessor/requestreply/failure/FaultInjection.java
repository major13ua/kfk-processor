package xme.common.kfkprocessor.requestreply.failure;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.springframework.context.support.GenericApplicationContext;
import xme.common.kfkprocessor.requestreply.adapters.kafka.KafkaReplySink;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.engine.CommitRetry;
import xme.common.kfkprocessor.requestreply.ports.CommitResult;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.ReplyRecord;
import xme.common.kfkprocessor.requestreply.ports.ReplySink;

/**
 * Commit-side fault injection around the real Kafka sink. A failing call throws before anything is sent, which is
 * what a failed (aborted) transaction looks like to the engine: nothing visible, positions not advanced.
 */
final class FaultInjection implements ReplySink, AutoCloseable {

    private volatile KafkaReplySink real;
    final AtomicInteger commitCalls = new AtomicInteger();
    /** Number of upcoming commit calls that fail with a plain commit failure. */
    final AtomicInteger failNextCommits = new AtomicInteger();
    /** While true every commit and every probe reports the destination unavailable. */
    final AtomicBoolean destinationDown = new AtomicBoolean();
    final List<CommitRetry.Alert> alerts = new CopyOnWriteArrayList<>();

    @Override
    public CommitResult commit(List<ReplyRecord> replies) {
        commitCalls.incrementAndGet();
        if (destinationDown.get()) {
            throw new ReplyDestinationFault.Unavailable("injected: reply destination unavailable", null);
        }
        if (failNextCommits.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
            throw new IllegalStateException("injected: commit failed");
        }
        return real.commit(replies);
    }

    @Override
    public void close() {
        if (real != null) {
            real.close();
        }
    }

    /** Registers this sink (over the real Kafka sink), a probe following {@code destinationDown}, an alert recorder. */
    Consumer<GenericApplicationContext> beans(String group) {
        return ctx -> {
            ctx.registerBean("replySink", ReplySink.class, () -> {
                real = new KafkaReplySink(ctx.getBean(RequestReplyProperties.class),
                        FailureHarness.KAFKA.getBootstrapServers(), group);
                return this;
            });
            ctx.registerBean("requestReplyDestinationProbe", DestinationProbe.class, () -> () -> {
                if (destinationDown.get()) {
                    throw new ReplyDestinationFault.Unavailable("injected: reply destination unavailable", null);
                }
            });
            ctx.registerBean("requestReplyAlertListener", Consumer.class, () -> (Consumer<CommitRetry.Alert>) alerts::add);
        };
    }
}
