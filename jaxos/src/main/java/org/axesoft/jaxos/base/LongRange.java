package org.axesoft.jaxos.base;

public class LongRange implements Comparable<LongRange> {
    private long low;
    private long high;
    private long timestamp;

    public LongRange(long low, long high) {
        this.low = low;
        this.high = high;
        this.timestamp = 0;
    }

    public LongRange(long low, long high, long timestamp) {
        this.low = low;
        this.high = high;
        this.timestamp = timestamp;
    }

    public long low(){
        return this.low;
    }

    public long high(){
        return this.high;
    }

    public long timestamp() {
        return this.timestamp;
    }

    @Override
    public String toString() {
        return "LongRange{" + low +", " + high +'}';
    }


    @Override
    public int compareTo(LongRange o) {
        return Long.compare(this.low, o.low);
    }
}
