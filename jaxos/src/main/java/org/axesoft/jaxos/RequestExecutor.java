package org.axesoft.jaxos;

import com.google.common.util.concurrent.ListenableFuture;

public interface RequestExecutor {
    ListenableFuture<Void> submit(int squadId, Runnable r);
}
