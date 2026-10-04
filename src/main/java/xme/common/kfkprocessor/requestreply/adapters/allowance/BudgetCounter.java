package xme.common.kfkprocessor.requestreply.adapters.allowance;

/** Seam over the shared token bucket; implementations throw any runtime failure when the store is unreachable. */
public interface BudgetCounter {

    /** Consumes up to {@code max} tokens and returns how many were consumed (0..max). */
    long takeUpTo(long max);

    /** Adds tokens back to the bucket. */
    void put(long units);
}
