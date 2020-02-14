package org.axesoft.jaxos.algo;


public class CommunicatorException extends RuntimeException {
    public CommunicatorException() {
        super();
    }

    public CommunicatorException(String message) {
        super(message);
    }

    public CommunicatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public CommunicatorException(Throwable cause) {
        super(cause);
    }
}
