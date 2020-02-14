package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.axesoft.jaxos.base.DateFormater;

import java.io.Serializable;
import java.util.Date;


public class CheckPoint implements Serializable {
    public static CheckPoint EMPTY = new CheckPoint(0, 0, 0, ByteString.EMPTY, Instance.emptyOf(0));

    private int squadId;
    private long instanceId;
    private long timestamp;
    private ByteString content;
    private Instance lastInstance;

    public CheckPoint(int squadId, long instanceId, long timestamp, ByteString content, Instance lastInstance) {
        this.squadId = squadId;
        this.instanceId = instanceId;
        this.timestamp = timestamp;
        this.content = content;
        this.lastInstance = lastInstance;
    }

    public int squadId(){
        return this.squadId;
    }

    public long instanceId(){
        return this.instanceId;
    }

    public long timestamp(){
        return this.timestamp;
    }

    public ByteString content(){
        return this.content;
    }

    public boolean isEmpty(){
        return this.instanceId == 0;
    }

    public Instance lastInstance(){
        return this.lastInstance;
    }

    @Override
    public String toString() {
        return String.format("CheckPoint{" +
                "squadId=" + squadId +
                ", version=" + instanceId +
                ", timestamp=" + new DateFormater().format(new Date(timestamp))+
                ", content=BX[" + content.size() + "]" +
                ", lastInstance=" + this.lastInstance +
                '}');
    }
}
