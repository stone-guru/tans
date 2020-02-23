package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

public interface StateMachine {

    interface Snapshot {

    }
    void consume(int squadId, long instanceId, ByteString message);

    void close();

    ByteString makeCheckPoint(int squadId);

    void restoreFromCheckPoint(int squadId, long instanceId, ByteString checkPoint);

    Snapshot getSnapshot(int squad);
}
