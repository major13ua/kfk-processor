package xme.common.kfkprocessor.requestreply.adapters.allowance;

import xme.common.kfkprocessor.requestreply.api.AllowanceStoreUnavailableException;
import xme.common.kfkprocessor.requestreply.ports.AllowanceStore;

/** Redis-compatible AllowanceStore adapter (ADR-0003); maps every store failure to AllowanceStoreUnavailableException. */
public class RedisAllowanceStore implements AllowanceStore {

    private final BudgetCounter counter;

    public RedisAllowanceStore(BudgetCounter counter) {
        this.counter = counter;
    }

    @Override
    public long reserve(long units) {
        if (units <= 0) {
            return 0;
        }
        try {
            return Math.max(0, Math.min(units, counter.takeUpTo(units)));
        } catch (RuntimeException e) {
            throw new AllowanceStoreUnavailableException("Rate budget store unavailable", e);
        }
    }

    @Override
    public void giveBack(long units) {
        if (units <= 0) {
            return;
        }
        try {
            counter.put(units);
        } catch (RuntimeException e) {
            // Unit stays consumed: lowers throughput, never exceeds the budget.
        }
    }
}
