package org.axesoft.jaxos.network;

public class CodingException extends RuntimeException {
    public CodingException(String message) {
        super(message);
    }

    public CodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public CodingException(Throwable cause) {
        super(cause);
    }
}
