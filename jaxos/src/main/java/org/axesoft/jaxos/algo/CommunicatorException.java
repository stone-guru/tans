package org.axesoft.jaxos.algo;

/**
 * @author gaoyuan
 * @sine 2019/9/3.
 */
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
