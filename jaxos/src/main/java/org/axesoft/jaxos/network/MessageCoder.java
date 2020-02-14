package org.axesoft.jaxos.network;

import org.axesoft.jaxos.algo.Event;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public interface MessageCoder<T> {
    T encode(Event event);

    Event decode(T t);
}


