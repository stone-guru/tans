package org.axesoft.jaxos.algo;

public class NoQuorumException extends RuntimeException {
    public NoQuorumException() {
    }

    public NoQuorumException(String message) {
        super(message);
    }

    public NoQuorumException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoQuorumException(Throwable cause) {
        super(cause);
    }
}
