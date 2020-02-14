package org.axesoft.jaxos.algo;

import io.netty.util.Timeout;

import java.util.concurrent.TimeUnit;

public interface EventTimer {
    Timeout createTimeout(long duration, TimeUnit timeUnit, Event timeoutEvent);
}
