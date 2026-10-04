package xme.common.kfkprocessor.requestreply.autoconfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;
import xme.common.kfkprocessor.requestreply.engine.LaneShares;

/** Validates configuration at startup; refuses with plain-language messages naming the conflict. */
public class StartupValidator {

    private static final String PREFIX = "request_reply.config.";

    private final Consumer<String> log;

    public StartupValidator(Consumer<String> log) {
        this.log = log;
    }

    /** @throws ConfigurationRefusedException on the first violated rule; logs effective lane shares when valid. */
    public void validate(RequestReplyProperties props, int handlerCount) {
        if (handlerCount != 1) {
            throw refuse("handler_missing",
                    "Exactly one Handler is required per worker but " + handlerCount + " were found.");
        }
        String id = props.getWorkerIdentity();
        if (id == null || id.isBlank()) {
            throw refuse("identity_missing",
                    "A stable identity must be configured: set xme.request-reply.worker-identity explicitly.");
        }
        Map<String, Double> weights = new LinkedHashMap<>();
        for (RequestReplyProperties.Lane lane : props.getLanes()) {
            double w = lane.getWeight();
            if (!Double.isFinite(w) || w <= 0) {
                throw refuse("weight_not_positive",
                        "Priority Weight of lane '" + lane.getName() + "' is " + w + " but must be above 0.");
            }
            weights.put(lane.getName(), w);
        }
        var timeout = props.getHandlerTimeout();
        var deadline = props.effectiveCycleDeadline();
        var window = props.getCommitWindow();
        if (timeout.compareTo(deadline) > 0) {
            throw refuse("timeout_exceeds_cycle_deadline",
                    "Handler timeout " + timeout.toSeconds() + "s does not fit inside the Cycle deadline "
                            + deadline.toSeconds() + "s.");
        }
        if (deadline.compareTo(window.multipliedBy(80).dividedBy(100)) > 0) {
            throw refuse("cycle_deadline_exceeds_commit_window_share",
                    "Cycle deadline " + deadline.toSeconds() + "s exceeds 80% of the commit window "
                            + window.toSeconds() + "s.");
        }
        LaneShares.effectiveShares(weights, props.getMinLaneSharePercent() / 100.0).forEach((name, share) -> {
            double configured = weights.get(name) / weights.values().stream().mapToDouble(Double::doubleValue).sum();
            String note = share > configured + 1e-9 ? " (raised to minimum share)" : "";
            log.accept(String.format("Lane '%s' effective share %.1f%%%s", name, share * 100, note));
        });
    }

    private static ConfigurationRefusedException refuse(String suffix, String message) {
        String code = PREFIX + suffix;
        return new ConfigurationRefusedException(code, message + " [" + code + "]");
    }
}
