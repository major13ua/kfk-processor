package xme.common.kfkprocessor.requestreply.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds {@code xme.request-reply.*}; defaults per public-api.md section 3. */
@ConfigurationProperties("xme.request-reply")
public class RequestReplyProperties {

    public static class Lane {
        private String name;
        private String source;
        private double weight;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getSource() { return source; }
        public void setSource(String source) { this.source = source; }
        public double getWeight() { return weight; }
        public void setWeight(double weight) { this.weight = weight; }
    }

    public static class AllowanceStore {
        private String redisUri;

        public String getRedisUri() { return redisUri; }
        public void setRedisUri(String v) { this.redisUri = v; }
    }

    private boolean autoStart = true;
    private String groupId;
    private long maxPayloadBytes = 1_048_576;
    private int drawPerRound = 100;
    private Duration probeInterval = Duration.ofSeconds(5);
    private AllowanceStore allowanceStore = new AllowanceStore();
    private String workerIdentity;
    private String replyDestination;
    private long rateBudgetPerSecond;
    private List<Lane> lanes = new ArrayList<>();
    private Integer minLaneSharePercent = 5;
    private Duration handlerTimeout = Duration.ofSeconds(30);
    private Duration cycleDeadline;
    private Duration commitWindow = Duration.ofSeconds(60);
    private Integer commitRetryAttempts = 3;
    private Duration identityWindow = Duration.ofSeconds(45);
    private Duration stallThreshold = Duration.ofSeconds(60);

    public String getWorkerIdentity() { return workerIdentity; }
    public void setWorkerIdentity(String v) { this.workerIdentity = v; }
    public String getReplyDestination() { return replyDestination; }
    public void setReplyDestination(String v) { this.replyDestination = v; }
    public long getRateBudgetPerSecond() { return rateBudgetPerSecond; }
    public void setRateBudgetPerSecond(long v) { this.rateBudgetPerSecond = v; }
    public List<Lane> getLanes() { return lanes; }
    public void setLanes(List<Lane> v) { this.lanes = v; }
    public boolean isAutoStart() { return autoStart; }
    public void setAutoStart(boolean v) { this.autoStart = v; }
    /** Explicit group id, else {@code request-reply-<reply-destination>}. */
    public String getGroupId() { return groupId != null ? groupId : "request-reply-" + replyDestination; }
    public void setGroupId(String v) { this.groupId = v; }
    public long getMaxPayloadBytes() { return maxPayloadBytes; }
    public void setMaxPayloadBytes(long v) { this.maxPayloadBytes = v; }
    public int getDrawPerRound() { return drawPerRound; }
    public void setDrawPerRound(int v) { this.drawPerRound = v; }
    public Duration getProbeInterval() { return probeInterval; }
    public void setProbeInterval(Duration v) { this.probeInterval = v; }
    public AllowanceStore getAllowanceStore() { return allowanceStore; }
    public void setAllowanceStore(AllowanceStore v) { this.allowanceStore = v; }
    /** Contract name (public-api.md section 3) of {@link #getMinLaneSharePercent()}. */
    public Integer getMinLaneShare() { return minLaneSharePercent; }
    public void setMinLaneShare(Integer v) { this.minLaneSharePercent = v; }
    public Integer getMinLaneSharePercent() { return minLaneSharePercent; }
    public void setMinLaneSharePercent(Integer v) { this.minLaneSharePercent = v; }
    public Duration getHandlerTimeout() { return handlerTimeout; }
    public void setHandlerTimeout(Duration v) { this.handlerTimeout = v; }
    /** Explicit value, or null when unset (then {@link #effectiveCycleDeadline()} applies). */
    public Duration getCycleDeadline() { return cycleDeadline; }
    public void setCycleDeadline(Duration v) { this.cycleDeadline = v; }
    public Duration getCommitWindow() { return commitWindow; }
    public void setCommitWindow(Duration v) { this.commitWindow = v; }
    public Integer getCommitRetryAttempts() { return commitRetryAttempts; }
    public void setCommitRetryAttempts(Integer v) { this.commitRetryAttempts = v; }
    public Duration getIdentityWindow() { return identityWindow; }
    public void setIdentityWindow(Duration v) { this.identityWindow = v; }
    public Duration getStallThreshold() { return stallThreshold; }
    public void setStallThreshold(Duration v) { this.stallThreshold = v; }

    /** Explicit cycle-deadline, else 80% of commit-window. */
    public Duration effectiveCycleDeadline() {
        return cycleDeadline != null ? cycleDeadline : commitWindow.multipliedBy(80).dividedBy(100);
    }
}
