package xme.common.kfkprocessor.requestreply.autoconfig;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.RequestReplyProperties;

class StartupValidatorTest {

    private final List<String> log = new ArrayList<>();
    private final StartupValidator validator = new StartupValidator(log::add);

    private static RequestReplyProperties.Lane lane(String name, double weight) {
        RequestReplyProperties.Lane l = new RequestReplyProperties.Lane();
        l.setName(name);
        l.setSource(name + "-topic");
        l.setWeight(weight);
        return l;
    }

    private static RequestReplyProperties valid() {
        RequestReplyProperties p = new RequestReplyProperties();
        p.setWorkerIdentity("worker-1");
        p.setReplyDestination("replies");
        p.setRateBudgetPerSecond(100);
        p.setLanes(List.of(lane("a", 5), lane("b", 3), lane("c", 2)));
        p.setHandlerTimeout(Duration.ofSeconds(30));
        p.setCommitWindow(Duration.ofSeconds(60));
        p.setCycleDeadline(Duration.ofSeconds(48));
        return p;
    }

    private ConfigurationRefusedException refused(RequestReplyProperties p, int handlers) {
        return assertThrows(ConfigurationRefusedException.class, () -> validator.validate(p, handlers));
    }

    // AC-02 valid
    @Test
    void validConfigurationStarts() {
        assertDoesNotThrow(() -> validator.validate(valid(), 1));
    }

    // AC-02: timeout does not fit Cycle deadline
    @Test
    void timeoutLongerThanCycleDeadlineIsRefusedNamingBothValues() {
        RequestReplyProperties p = valid();
        p.setHandlerTimeout(Duration.ofSeconds(50));
        p.setCycleDeadline(Duration.ofSeconds(40));
        ConfigurationRefusedException e = refused(p, 1);
        assertEquals("request_reply.config.timeout_exceeds_cycle_deadline", e.code());
        assertTrue(e.getMessage().contains("request_reply.config.timeout_exceeds_cycle_deadline"), e.getMessage());
        assertTrue(e.getMessage().contains("50"), e.getMessage());
        assertTrue(e.getMessage().contains("40"), e.getMessage());
    }

    // AC-02: Cycle deadline above 80% of commit window
    @Test
    void cycleDeadlineAbove80PercentOfCommitWindowIsRefused() {
        RequestReplyProperties p = valid();
        p.setCycleDeadline(Duration.ofSeconds(50));
        ConfigurationRefusedException e = refused(p, 1);
        assertEquals("request_reply.config.cycle_deadline_exceeds_commit_window_share", e.code());
        assertTrue(e.getMessage().contains("50"), e.getMessage());
        assertTrue(e.getMessage().contains("60"), e.getMessage());
    }

    @Test
    void cycleDeadlineExactly80PercentIsAccepted() {
        RequestReplyProperties p = valid();
        p.setCycleDeadline(Duration.ofSeconds(48));
        assertDoesNotThrow(() -> validator.validate(p, 1));
    }

    // AC-02: weight not positive
    @Test
    void zeroWeightIsRefusedNamingTheLane() {
        RequestReplyProperties p = valid();
        p.setLanes(List.of(lane("a", 5), lane("urgent", 0)));
        ConfigurationRefusedException e = refused(p, 1);
        assertEquals("request_reply.config.weight_not_positive", e.code());
        assertTrue(e.getMessage().contains("urgent"), e.getMessage());
    }

    @Test
    void negativeWeightIsRefused() {
        RequestReplyProperties p = valid();
        p.setLanes(List.of(lane("a", -1)));
        assertEquals("request_reply.config.weight_not_positive", refused(p, 1).code());
    }

    // AC-02: no Handler, or more than one
    @Test
    void noHandlerIsRefused() {
        ConfigurationRefusedException e = refused(valid(), 0);
        assertEquals("request_reply.config.handler_missing", e.code());
        assertTrue(e.getMessage().toLowerCase().contains("handler"), e.getMessage());
    }

    @Test
    void moreThanOneHandlerIsRefused() {
        assertEquals("request_reply.config.handler_missing", refused(valid(), 2).code());
    }

    // AC-15
    @Test
    void missingWorkerIdentityIsRefusedExplainingStableIdentity() {
        RequestReplyProperties p = valid();
        p.setWorkerIdentity(null);
        ConfigurationRefusedException e = refused(p, 1);
        assertEquals("request_reply.config.identity_missing", e.code());
        assertTrue(e.getMessage().toLowerCase().contains("stable identity"), e.getMessage());
    }

    @Test
    void blankWorkerIdentityIsRefused() {
        RequestReplyProperties p = valid();
        p.setWorkerIdentity("  ");
        assertEquals("request_reply.config.identity_missing", refused(p, 1).code());
    }

    // AC-02: valid config logs effective share per lane; lane below 5% raised, others scaled
    @Test
    void validConfigurationLogsEffectiveSharePerLane() {
        validator.validate(valid(), 1);
        String all = String.join("\n", log);
        assertTrue(all.contains("a") && all.contains("50"), all);
        assertTrue(all.contains("b") && all.contains("30"), all);
        assertTrue(all.contains("c") && all.contains("20"), all);
    }

    @Test
    void laneBelowMinimumIsLoggedAsRaisedTo5Percent() {
        RequestReplyProperties p = valid();
        p.setLanes(List.of(lane("big", 99), lane("tiny", 1)));
        validator.validate(p, 1);
        String all = String.join("\n", log);
        assertTrue(all.contains("tiny") && all.contains("5"), all);
        assertTrue(all.contains("big") && all.contains("95"), all);
    }
}
