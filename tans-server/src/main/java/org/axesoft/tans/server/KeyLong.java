package org.axesoft.tans.server;

public class KeyLong {
    private String key;
    private long value;

    public KeyLong(String key, long value) {
        this.key = key;
        this.value = value;
    }

    public String key(){
        return this.key;
    }

    public long value(){
        return this.value;
    }

    @Override
    public String toString() {
        return "KeyLong{'" + key + '\'' + value + '}';
    }
}
