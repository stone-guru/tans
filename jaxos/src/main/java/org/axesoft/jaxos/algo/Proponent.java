package org.axesoft.jaxos.algo;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import java.util.concurrent.CompletableFuture;

public interface Proponent extends HasMetrics {
    CompletableFuture<ProposeResult<StateMachine.Snapshot>> propose(int squadId, ByteString v, boolean ignoreLeader);
}
