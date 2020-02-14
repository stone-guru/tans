package org.axesoft.jaxos.algo;

/**
 * @author bison
 * @sine 2020/1/6.
 */
public interface SquadMetrics {
    enum ProposalResult {
        SUCCESS, CONFLICT, OTHER
    }
    void recordAccept(long nanos);
    void recordPropose(long nanos, ProposalResult result);
    void recordLearnMillis(long millis);
    void recordTeachNanos(long nanos);
    void recordLeader(int serverId);
    void incPeerTimeoutCounter();
}
