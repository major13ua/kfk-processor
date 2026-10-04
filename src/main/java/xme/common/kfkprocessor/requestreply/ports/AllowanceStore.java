package xme.common.kfkprocessor.requestreply.ports;

import xme.common.kfkprocessor.requestreply.api.AllowanceStoreUnavailableException;

/** Public SPI: shared Rate Budget counter (ADR-0003, ADR-0006). */
public interface AllowanceStore {

    /** Reserves up to {@code units}; may grant less (including 0). Fails closed when the store is unreachable. */
    long reserve(long units) throws AllowanceStoreUnavailableException;

    /** Returns unused allowance to the shared budget. */
    void giveBack(long units);
}
