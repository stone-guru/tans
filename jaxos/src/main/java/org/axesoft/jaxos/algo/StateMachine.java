package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

public interface StateMachine {

    interface Snapshot {

    }

    long currentVersion(int squadId);

    void consume(int squadId, long instanceId, ByteString message);

    void close();

    Pair<ByteString, Long> makeCheckPoint(int squadId);

    void restoreFromCheckPoint(int squadId, long version, ByteString checkPoint);

    Snapshot getSnapshot(int squad);
}
