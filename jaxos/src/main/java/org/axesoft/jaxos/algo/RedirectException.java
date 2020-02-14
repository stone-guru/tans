package org.axesoft.jaxos.algo;

public class RedirectException extends  RuntimeException {
    private int serverId;
    public RedirectException(int serverId) {
        super();
        this.serverId = serverId;
    }

    public int getServerId(){
        return this.serverId;
    }

    @Override
    public String getMessage() {
        return "Redirect to " + this.serverId;
    }
}
