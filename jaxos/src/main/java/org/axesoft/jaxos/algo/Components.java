package org.axesoft.jaxos.algo;


public interface Components {
    Communicator getCommunicator();

    AcceptorLogger getLogger();

    EventWorkerPool getWorkerPool();

    default EventTimer getEventTimer() {
        return getWorkerPool();
    }

    JaxosMetrics getJaxosMetrics();
}
