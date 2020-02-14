package org.axesoft.jaxos.algo;


public interface Communicator  {
    /**
     * Is more than n/2 node connected
     */
    boolean available();
    void broadcast(Event msg);
    void broadcastOthers(Event msg);
    void selfFirstBroadcast(Event msg);
    void send(Event event, int serverId);
    void close();
}
