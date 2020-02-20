package org.axesoft.jaxos.algo;

import java.util.function.Supplier;

public interface SquadMetrics {
    enum ProposalResult {
        SUCCESS, CONFLICT, OTHER
    }

    void incProposeRequestCounter();
    void recordAccept(long nanos);
    void recordPropose(long nanos, ProposalResult result);
    void recordLearnMillis(long millis);
    void recordTeachNanos(long nanos);
    void incPeerTimeoutCounter();
    void createProposeQueueSizeIfNotSet(Supplier<Number> sizeSupplier);
    void createLeaderGaugeIfNotSet(Supplier<Number> leaderSupplier);
    void createInstanceIdGaugeIfNotSet(Supplier<Number> leaderSupplier);
}
