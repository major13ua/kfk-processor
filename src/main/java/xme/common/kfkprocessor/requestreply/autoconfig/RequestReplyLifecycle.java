package xme.common.kfkprocessor.requestreply.autoconfig;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import org.springframework.context.SmartLifecycle;
import xme.common.kfkprocessor.requestreply.engine.CommitRetry;
import xme.common.kfkprocessor.requestreply.engine.CycleLoop;
import xme.common.kfkprocessor.requestreply.engine.WorkerState;
import xme.common.kfkprocessor.requestreply.ports.DestinationProbe;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;
import xme.common.kfkprocessor.requestreply.ports.RequestLanes;

/**
 * Starts and stops the Cycle loop. At start the reply destination is probed: when write permission is missing the
 * worker is paused(permission), reports the configuration fault, keeps polling with every lane paused (so it stays
 * in its group with the same lanes) and starts the loop by itself once the probe succeeds.
 */
final class RequestReplyLifecycle implements SmartLifecycle {

    private static final System.Logger LOG = System.getLogger(RequestReplyLifecycle.class.getName());
    private static final String PERMISSION_DENIED = "request_reply.reply_destination.permission_denied";

    private final CycleLoop<?, ?, ?> loop;
    private final RequestLanes lanes;
    private final DestinationProbe probe;
    private final WorkerState state;
    private final Consumer<CommitRetry.Alert> alert;
    private final Duration probeInterval;
    private final boolean autoStart;
    private volatile boolean running;
    private Thread gate;

    RequestReplyLifecycle(CycleLoop<?, ?, ?> loop, RequestLanes lanes, DestinationProbe probe, WorkerState state,
            Consumer<CommitRetry.Alert> alert, Duration probeInterval, boolean autoStart) {
        this.loop = loop;
        this.lanes = lanes;
        this.probe = probe;
        this.state = state;
        this.alert = alert;
        this.probeInterval = probeInterval;
        this.autoStart = autoStart;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        try {
            probe.probe();
        } catch (ReplyDestinationFault.PermissionDenied denied) {
            state.pause(WorkerState.PauseReason.PERMISSION);
            lanes.pause();
            alert.accept(new CommitRetry.Alert(WorkerState.PauseReason.PERMISSION, PERMISSION_DENIED));
            gate = new Thread(this::awaitPermission, "request-reply-permission-gate");
            gate.setDaemon(true);
            gate.start();
            return;
        } catch (RuntimeException unavailable) {
            LOG.log(System.Logger.Level.WARNING, "Reply destination probe failed at start: "
                    + unavailable.getClass().getName());
        }
        loop.start();
    }

    private void awaitPermission() {
        long next = System.nanoTime() + probeInterval.toNanos();
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                lanes.fetch(Map.of()); // all lanes paused: keeps the group membership alive, consumes nothing
                if (System.nanoTime() - next < 0) {
                    continue;
                }
                next = System.nanoTime() + probeInterval.toNanos();
                probe.probe();
            } catch (ReplyDestinationFault | org.apache.kafka.common.errors.InterruptException e) {
                continue;
            } catch (RuntimeException e) {
                if (!running) {
                    return;
                }
                continue;
            }
            synchronized (this) {
                if (!running) {
                    return;
                }
                state.resume(WorkerState.PauseReason.PERMISSION);
                lanes.resume();
                loop.start();
            }
            return;
        }
    }

    @Override
    public void stop() {
        Thread g;
        synchronized (this) {
            running = false;
            g = gate;
            gate = null;
        }
        if (g != null) {
            g.interrupt();
            try {
                g.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        loop.stop();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return autoStart;
    }
}
