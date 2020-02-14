package org.axesoft.jaxos.algo;

import org.slf4j.Logger;

public class RunnableWithLog implements Runnable {
    private int squadId;
    private Runnable r;
    private Logger logger;

    public RunnableWithLog(Logger logger, Runnable r) {
        this(-1, logger, r);
    }

    public RunnableWithLog(int squadId, Logger logger, Runnable r) {
        this.squadId = squadId;
        this.r = r;
        this.logger = logger;
    }

    @Override
    public void run() {
        try {
            r.run();
        }
        catch (Exception e) {
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw e;
            }
            else {
                String msg = squadId >= 0 ? "Exec error" : "S " + squadId + " Exec ballot task error";
                logger.error(msg, e);
            }
        }
    }
}
