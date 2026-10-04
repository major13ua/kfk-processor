package xme.common.kfkprocessor.requestreply.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class IdempotencyKeyTest {

    @Test
    void keyHasLanePartitionPositionFormat() {
        assertEquals("gold:3:42", IdempotencyKey.of("gold", 3, 42L));
    }

    @Test
    void sameRequestIdentityGivesSameKeyOnReExecution() {
        assertEquals(IdempotencyKey.of("gold", 3, 42L), IdempotencyKey.of("gold", 3, 42L));
    }

    @Test
    void keyDiffersByLane() {
        assertNotEquals(IdempotencyKey.of("gold", 3, 42L), IdempotencyKey.of("silver", 3, 42L));
    }

    @Test
    void keyDiffersByPartition() {
        assertNotEquals(IdempotencyKey.of("gold", 3, 42L), IdempotencyKey.of("gold", 4, 42L));
    }

    @Test
    void keyDiffersByPositionInSameLaneAndPartition() {
        assertNotEquals(IdempotencyKey.of("gold", 3, 42L), IdempotencyKey.of("gold", 3, 43L));
    }
}
