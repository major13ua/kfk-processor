package xme.common.kfkprocessor.requestreply.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RequestReplyPropertiesTest {

    // AC-02 / public-api.md §3 defaults
    @Test
    void defaultsMatchContract() {
        RequestReplyProperties p = new RequestReplyProperties();
        assertEquals(5, p.getMinLaneSharePercent());
        assertEquals(Duration.ofSeconds(30), p.getHandlerTimeout());
        assertEquals(Duration.ofSeconds(60), p.getCommitWindow());
        assertEquals(3, p.getCommitRetryAttempts());
        assertEquals(Duration.ofSeconds(45), p.getIdentityWindow());
        assertEquals(Duration.ofSeconds(60), p.getStallThreshold());
        assertNull(p.getWorkerIdentity());
    }

    @Test
    void unsetCycleDeadlineDerivesTo80PercentOfCommitWindow() {
        RequestReplyProperties p = new RequestReplyProperties();
        assertEquals(Duration.ofSeconds(48), p.effectiveCycleDeadline());
    }

    @Test
    void explicitCycleDeadlineWins() {
        RequestReplyProperties p = new RequestReplyProperties();
        p.setCycleDeadline(Duration.ofSeconds(40));
        assertEquals(Duration.ofSeconds(40), p.effectiveCycleDeadline());
    }
}
