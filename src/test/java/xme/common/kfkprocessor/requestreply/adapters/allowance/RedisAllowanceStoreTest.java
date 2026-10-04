package xme.common.kfkprocessor.requestreply.adapters.allowance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.api.AllowanceStoreUnavailableException;

class RedisAllowanceStoreTest {

    /** Fake bucket with a fixed capacity. */
    static class FakeCounter implements BudgetCounter {
        long tokens;
        boolean down;
        long lastAsked;

        FakeCounter(long tokens) {
            this.tokens = tokens;
        }

        @Override
        public long takeUpTo(long max) {
            if (down) throw new IllegalStateException("connection refused");
            lastAsked = max;
            long g = Math.min(max, tokens);
            tokens -= g;
            return g;
        }

        @Override
        public void put(long units) {
            if (down) throw new IllegalStateException("connection refused");
            tokens += units;
        }
    }

    // AC-10
    @Test
    void reserveGrantsWhatIsAskedWhenAvailable() {
        var store = new RedisAllowanceStore(new FakeCounter(10));
        assertEquals(4, store.reserve(4));
    }

    // AC-10
    @Test
    void reserveNeverGrantsMoreThanAskedOrRemaining() {
        var c = new FakeCounter(3);
        var store = new RedisAllowanceStore(c);
        assertEquals(3, store.reserve(10));
        assertEquals(0, store.reserve(5));
    }

    // AC-10
    @Test
    void reserveClampsEvenIfCounterOverGrants() {
        var store = new RedisAllowanceStore(new BudgetCounter() {
            public long takeUpTo(long max) { return max + 7; }
            public void put(long units) {}
        });
        assertTrue(store.reserve(5) <= 5);
    }

    // AC-10
    @Test
    void giveBackReturnsUnitsToTheBudget() {
        var c = new FakeCounter(5);
        var store = new RedisAllowanceStore(c);
        assertEquals(5, store.reserve(5));
        store.giveBack(3);
        assertEquals(3, store.reserve(5));
    }

    // AC-18
    @Test
    void unreachableStoreThrowsInsteadOfGranting() {
        var c = new FakeCounter(10);
        c.down = true;
        var store = new RedisAllowanceStore(c);
        var ex = assertThrows(AllowanceStoreUnavailableException.class, () -> store.reserve(1));
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    // AC-18, edge case: failed return only lowers throughput
    @Test
    void giveBackFailureDoesNotPropagate() {
        var c = new FakeCounter(10);
        var store = new RedisAllowanceStore(c);
        c.down = true;
        store.giveBack(2);
    }
}
