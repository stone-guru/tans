package org.axesoft.tans.server;

public class KeyLong {
    private String key;
    private long value;
    private long stamp;

    public KeyLong(String key, long value) {
        this.key = key;
        this.value = value;
        this.stamp = 0L;
    }

    public KeyLong(String key, long value, long stamp) {
        this.key = key;
        this.value = value;
        this.stamp = stamp;
    }

    public String key(){
        return this.key;
    }

    public long value(){
        return this.value;
    }

    public long stamp() {
        return this.stamp;
    }

    @Override
    public String toString() {
        return "KeyLong{'" + key + '\'' + value + '}';
    }
}
