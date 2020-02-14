package org.axesoft.jaxos.algo;

/**
 * A integrated interface providing implementation of basic components to be shared by Acceptor, Proposer, etc
 *
 * @sine 2019/9/28.
 */
public interface Components {
    Communicator getCommunicator();

    AcceptorLogger getLogger();

    EventWorkerPool getWorkerPool();

    default EventTimer getEventTimer() {
        return getWorkerPool();
    }

    JaxosMetrics getJaxosMetrics();
}
