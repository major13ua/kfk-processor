package xme.common.kfkprocessor.requestreply.adapters.allowance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import xme.common.kfkprocessor.requestreply.api.AllowanceStoreUnavailableException;

@TestMethodOrder(OutageLastOrderer.class)
@Testcontainers(disabledWithoutDocker = true)
class RedisAllowanceStoreIT {

    private static final long BUDGET = 100;

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    private String uri() {
        return "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
    }

    // AC-10
    @Test
    void twoClientsShareOneBudgetAndOverReserveIsCapped() {
        try (var a = Bucket4jBudgetCounters.connect(uri(), "it-shared", BUDGET);
                var b = Bucket4jBudgetCounters.connect(uri(), "it-shared", BUDGET)) {
            var sa = new RedisAllowanceStore(a.counter());
            var sb = new RedisAllowanceStore(b.counter());
            long total = sa.reserve(60) + sb.reserve(60) + sa.reserve(1000);
            // 1.05 tolerance (spec §6) over one second window
            assertTrue(total <= BUDGET * 1.05, "granted " + total);
            assertEquals(0, sb.reserve(10));
        }
    }

    // AC-10
    @Test
    void giveBackIsVisibleToOtherClient() {
        try (var a = Bucket4jBudgetCounters.connect(uri(), "it-giveback", BUDGET);
                var b = Bucket4jBudgetCounters.connect(uri(), "it-giveback", BUDGET)) {
            var sa = new RedisAllowanceStore(a.counter());
            var sb = new RedisAllowanceStore(b.counter());
            long got = sa.reserve(BUDGET);
            sa.giveBack(got / 2);
            assertTrue(sb.reserve(BUDGET) >= got / 2 - 1);
        }
    }

    // AC-18
    @Test
    void stoppingTheStoreRaisesUnavailable() {
        try (var a = Bucket4jBudgetCounters.connect(uri(), "it-outage", BUDGET)) {
            var store = new RedisAllowanceStore(a.counter());
            assertEquals(1, store.reserve(1));
            redis.stop();
            assertThrows(AllowanceStoreUnavailableException.class, () -> store.reserve(1));
        }
    }
}
