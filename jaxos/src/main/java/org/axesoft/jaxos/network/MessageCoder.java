package org.axesoft.jaxos.network;

import org.axesoft.jaxos.algo.Event;

public interface MessageCoder<T> {
    T encode(Event event);

    Event decode(T t);
}


