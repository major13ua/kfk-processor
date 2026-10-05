package xme.common.kfkprocessor.requestreply.autoconfig;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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
        if (props.getReplyDestination() == null || props.getReplyDestination().isBlank()) {
            throw refuse("reply_destination_missing",
                    "No reply destination: set xme.request-reply.reply-destination.");
        }
        try {
            org.apache.kafka.common.internals.Topic.validate(props.getReplyDestination());
        } catch (org.apache.kafka.common.errors.InvalidTopicException e) {
            throw refuse("reply_destination_invalid",
                    "The reply destination '" + props.getReplyDestination() + "' is not a valid topic name: "
                            + e.getMessage());
        }
        if (props.getRateBudgetPerSecond() <= 0) {
            throw refuse("rate_budget_not_positive", "Rate Budget is " + props.getRateBudgetPerSecond()
                    + " per second but must be above 0: set xme.request-reply.rate-budget-per-second.");
        }
        if (props.getDrawPerRound() <= 0) {
            throw refuse("draw_per_round_not_positive",
                    "Draw per round is " + props.getDrawPerRound() + " but must be above 0.");
        }
        if (props.getLanes().isEmpty()) {
            throw refuse("lanes_missing", "At least one Priority Lane is required: set xme.request-reply.lanes.");
        }
        Set<String> names = new HashSet<>();
        Set<String> sources = new HashSet<>();
        for (RequestReplyProperties.Lane lane : props.getLanes()) {
            if (lane.getName() == null || lane.getName().isBlank()
                    || lane.getSource() == null || lane.getSource().isBlank()) {
                throw refuse("lane_incomplete", "Every lane needs a name and a source but lane '"
                        + lane.getName() + "' has source '" + lane.getSource() + "'.");
            }
            if (!names.add(lane.getName())) {
                throw refuse("lane_name_duplicate", "Lane name '" + lane.getName() + "' is used more than once.");
            }
            if (!sources.add(lane.getSource())) {
                throw refuse("lane_source_duplicate",
                        "Request source '" + lane.getSource() + "' is used by more than one lane.");
            }
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
                    "Handler timeout " + fmt(timeout) + " does not fit inside the Cycle deadline "
                            + fmt(deadline) + ".");
        }
        if (deadline.compareTo(window.multipliedBy(80).dividedBy(100)) > 0) {
            throw refuse("cycle_deadline_exceeds_commit_window_share",
                    "Cycle deadline " + fmt(deadline) + " exceeds 80% of the commit window "
                            + fmt(window) + ".");
        }
        LaneShares.effectiveShares(weights, props.getMinLaneSharePercent() / 100.0).forEach((name, share) -> {
            double configured = weights.get(name) / weights.values().stream().mapToDouble(Double::doubleValue).sum();
            String note = share > configured + 1e-9 ? " (raised to minimum share)" : "";
            log.accept(String.format("Lane '%s' effective share %.1f%%%s", name, share * 100, note));
        });
    }

    /**
     * The starter hands the Handler the Request Key and payload as UTF-8 strings and writes the reply with
     * {@code String.valueOf}; a Handler declared with any other type would fail at runtime. A null or {@code Object}
     * type is unresolved (lambda, raw type) and cannot be checked.
     */
    public void validateHandlerTypes(Class<?> key, Class<?> request, Class<?> response) {
        for (Class<?> type : new Class<?>[] {key, request, response}) {
            if (type != null && type != Object.class && type != String.class) {
                throw refuse("handler_types_unsupported",
                        "Handler types must be String (Request Key, request and reply are text) but found "
                                + describe(key) + ", " + describe(request) + ", " + describe(response)
                                + " for key, request and reply.");
            }
        }
    }

    private static String describe(Class<?> type) {
        return type == null ? "unresolved" : type.getSimpleName();
    }

    private static String fmt(java.time.Duration d) {
        return d.toMillis() % 1000 == 0 ? d.toSeconds() + "s" : d.toMillis() + "ms";
    }

    private static ConfigurationRefusedException refuse(String suffix, String message) {
        String code = PREFIX + suffix;
        return new ConfigurationRefusedException(code, message + " [" + code + "]");
    }
}
