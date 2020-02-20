package org.axesoft.jaxos.algo;


import java.util.function.Supplier;

public interface JaxosMetrics {
    SquadMetrics getOrCreateSquadMetrics(int squadId);
    void recordRestoreElapsedMillis(long millis);
    void recordLoggerLoadElapsed(long nanos);
    void recordLoggerSaveElapsed(long nanos);
    void recordLoggerSyncElapsed(long nanos);
    void recordLoggerDeleteElapsed(long nanos);
    void recordLoggerSaveCheckPointElapse(long nanos);
    void setLoggerDiskSizeSupplierIfNot(Supplier<Number> f);
    String format();
}
