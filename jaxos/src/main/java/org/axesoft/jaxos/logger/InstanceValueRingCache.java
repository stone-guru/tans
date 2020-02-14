package org.axesoft.jaxos.logger;

import com.google.common.collect.ImmutableList;
import org.axesoft.jaxos.algo.Instance;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @sine 2019/10/15.
 */
public class InstanceValueRingCache {
    private Instance[] buff;
    private int pos;

    public InstanceValueRingCache(int size) {
        checkArgument(size > 0, "RingBuff size should be positive %d", size);
        this.pos = 0;
        this.buff = new Instance[size];
    }

    public synchronized void put(Instance value) {
        int p0 = (this.pos - 1 + this.buff.length) % this.buff.length;
        Instance v0 = this.buff[p0];
        if (v0 != null) {
            if (v0.id() == value.id()) {
                this.buff[p0] = value;
                return;
            }
            else if (value.id() != v0.id() + 1){
                return;
            }
        }

        this.buff[this.pos] = value;
        this.pos = (this.pos + 1) % buff.length;
    }

    public synchronized int size() {
        if (buff[pos] == null) {
            return pos;
        }
        return buff.length;
    }

    public synchronized Optional<Instance> getLast() {
        int p0 = (this.pos - 1 + this.buff.length) % this.buff.length;
        return Optional.ofNullable(this.buff[p0]);
    }

    public synchronized List<Instance> get(long idLow, long idHigh) {
        int i0 = lessOrEqualPosOf(idHigh);

        if (i0 < 0) {
            return Collections.emptyList();
        }

        ImmutableList.Builder<Instance> builder = ImmutableList.builder();

        int i = i0;
        do {
            Instance v = this.buff[i];
            if (v == null || v.id() < idLow) {
                break;
            }
            else {
                builder.add(this.buff[i]);
                i = (i - 1 + this.buff.length) % this.buff.length;
            }
        } while (i != i0);

        return builder.build().reverse();
    }

    private int lessOrEqualPosOf(long instanceId) {
        int i = (this.pos - 1 + this.buff.length) % this.buff.length;

        while (true) {
            Instance v = this.buff[i];
            if (v == null) {
                return -1;
            }
            else if (v.id() <= instanceId) {
                return i;
            }
            else if (i == this.pos) {
                return -1;
            }
            else {
                i = (i - 1 + this.buff.length) % this.buff.length;
            }
        }
    }
}
