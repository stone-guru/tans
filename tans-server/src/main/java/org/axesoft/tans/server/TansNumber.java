package org.axesoft.tans.server;

public class TansNumber {
    private String name;
    private long version;
    private long timestamp;
    private long value;
    private long version0;
    private long value0;

    public TansNumber(String name, long value){
        this(name, 0, System.currentTimeMillis(), value);
    }

    public TansNumber(String name, long version, long timestamp, long value) {
        this.name = name;
        this.version = version;
        this.timestamp = timestamp;
        this.value = value;
    }

    public TansNumber(String name, long version, long timestamp, long value, long version0, long value0) {
        this.name = name;
        this.version = version;
        this.timestamp = timestamp;
        this.value = value;
        this.version0 = version0;
        this.value0 = value0;
    }

    public String name(){
        return this.name;
    }

    public long version(){
        return this.version;
    }

    public long timestamp(){
        return this.timestamp;
    }

    public long value(){
        return value;
    }

    public long version0(){
        return this.version0;
    }

    public long value0(){
        return this.value0;
    }

    public TansNumber update(long v){
        TansNumber n = new TansNumber(this.name, this.version + 1, System.currentTimeMillis(), this.value + v);
        n.value0 = this.value;
        n.version0 = this.version;
        return n;
    }

    @Override
    public String toString() {
        return "TansNumber{" +
                "name='" + name + '\'' +
                ", version=" + version +
                ", timestamp=" + timestamp +
                ", value=" + value +
                ", version0=" + version0 +
                ", value0=" + value0 +
                '}';
    }
}
