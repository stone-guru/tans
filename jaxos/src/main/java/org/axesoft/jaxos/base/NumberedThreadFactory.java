package org.axesoft.jaxos.base;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NumberedThreadFactory implements ThreadFactory {
    private AtomicInteger number = new AtomicInteger(0);

    private final String threadGroupName;

    public NumberedThreadFactory(String threadGroupName) {
        this.threadGroupName = threadGroupName;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, threadGroupName + "-" + number.incrementAndGet());
        t.setDaemon(true);
        return t;
    }
}
