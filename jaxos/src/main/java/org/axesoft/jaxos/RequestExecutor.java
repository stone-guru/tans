package org.axesoft.jaxos;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author bison
 * @sine 2020/1/16.
 */
public interface RequestExecutor {
    ListenableFuture<Void> submit(int squadId, Runnable r);
}
