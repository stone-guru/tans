package org.axesoft.jaxos.base;

public class LongRange implements Comparable<LongRange> {
    private long low;
    private long high;

    public LongRange(long low, long high) {
        this.low = low;
        this.high = high;
    }

    public long low(){
        return this.low;
    }

    public long high(){
        return this.high;
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
