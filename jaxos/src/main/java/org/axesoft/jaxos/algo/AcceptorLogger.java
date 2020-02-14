package org.axesoft.jaxos.algo;


public interface AcceptorLogger {

    void saveInstance(int squadId, long instanceId, int proposal, Event.BallotValue value);

    /**
     * load the last saved promise
     *
     * @param squadId
     * @return not null, {@link Instance#emptyOf(int)} if no such instance
     * @exception RuntimeException raised exception will be regard as empty instance
     */
    Instance loadLastInstance(int squadId);

    Instance loadInstance(int squadId, long instanceId);

    void saveCheckPoint(CheckPoint checkPoint, boolean deleteOldInstances);

    CheckPoint loadLastCheckPoint(int squadId);

    void sync();

    void close();
}
