package org.axesoft.jaxos.base;

import com.google.common.collect.ImmutableMap;

import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntBinaryOperator;

import static com.google.common.base.Preconditions.checkArgument;

public class SlideWindowMetric {
    private final int unitSeconds;
    private final int minutes;
    private final int prefMaxSize;
    private final int initValue;

    private AtomicReference<Node> head;
    private AtomicInteger size;
    private AtomicBoolean cutting;
    private StatisticMethod method;

    public SlideWindowMetric(int m, StatisticMethod statMethod, int unitSec) {
        Objects.requireNonNull(statMethod);
        checkArgument(unitSec > 0 && unitSec <= 60);
        checkArgument(m > 0);


        this.minutes = m;
        this.method = statMethod;
        this.unitSeconds = unitSec;
        this.prefMaxSize = m * 60 / this.unitSeconds;
        this.initValue = initValues.get(this.method);

        this.head = new AtomicReference<>(new Node(0, this.initValue, null));
        this.size = new AtomicInteger(1);
        this.cutting = new AtomicBoolean(false);
    }

    public void recordForPresent(int d) {
        recordForSecond(System.currentTimeMillis() / 1000, d);
    }

    public void recordAtMillis(long millis, int v) {
        recordForSecond(millis / 1000, v);
    }

    public void recordForSecond(long timeSecond, int v) {
        Node n = searchOrNewNode(timeSecond);
        if (n != null) {
            acceptv(n.value, v);
            if (this.size.get() >= prefMaxSize + prefMaxSize / 2) {
                trunc();
            }
        }
    }

    private Node searchOrNewNode(long timeSecond) {
        long tinkle = timeSecond / this.unitSeconds;
        while (true) {
            Node head = this.head.get();
            if (head.t == tinkle) {
                return head;
            }
            if (tinkle < head.t) {
                if (head.t - tinkle >= this.prefMaxSize) {
                    return null;
                }
                Node node = head;
                while (node != null && node.t > tinkle) {
                    node = node.n;
                }
                if (node != null && node.t == tinkle) {
                    return node;
                }
                return null;
            }
            else {
                long t = (tinkle >= head.t + prefMaxSize) ? tinkle - prefMaxSize + 1 : head.t + 1;
                Node nh = new Node(t, this.initValue, head);
                if (this.head.compareAndSet(head, nh)) {
                    size.getAndIncrement();
                }
            }
        }
    }

    private void trunc() {
        if (cutting.compareAndSet(false, true)) {
            try {
                Node n = head.get();
                Node prev = null;
                long t = n.t - this.prefMaxSize;
                while (n != null && n.t > t) {
                    prev = n;
                    n = n.n;
                }

                if (prev != null) {
                    prev.n = null;
                    int k = 0;
                    while (n != null) {
                        k++;
                        n = n.n;
                    }
                    size.getAndAdd(-k);
                }
            }
            finally {
                cutting.set(false);
            }
        }
    }

    public int getSum(long startSecond) {
        return accumulate(startSecond, 0, (a, b) -> a + b, 0);
    }

    public int getSum() {
        return getSum(System.currentTimeMillis() / 1000);
    }


    public int getMax(long startSecond, int deft) {
        return accumulate(startSecond, Integer.MIN_VALUE, Math::max, deft);
    }

    public int getMax(int deft) {
        return getMax(System.currentTimeMillis() / 1000, deft);
    }

    public int getMin(long startSecond, int deft) {
        return accumulate(startSecond, Integer.MAX_VALUE, Math::min, deft);
    }

    public int getMin(int deft) {
        return getMin(System.currentTimeMillis() / 1000, deft);
    }

    public double getAvgForSecond() {
        return getAvgForSecond(System.currentTimeMillis() / 1000);
    }

    public double getAvgForSecond(long startSecond) {
        double s = getSum(startSecond);
        return s / (minutes * 60);
    }

    public int tinkleCount() {
        int k = 0;
        Node n = head.get();
        while (n != null) {
            k++;
            n = n.n;
        }
        return k;
    }

    private void acceptv(AtomicInteger atomicInt, int v) {
        switch (this.method) {
            case STATMAX:
                atomicInt.updateAndGet(v0 -> Math.max(v0, v));
                return;
            case STATMIN:
                atomicInt.updateAndGet(v0 -> Math.min(v0, v));
                return;
            case STATSUM:
                atomicInt.getAndAdd(v);
        }
    }


    private Date tinkleToDate(long tinkle) {
        return new Date(tinkle * this.unitSeconds * 1000);
    }

    private int accumulate(long fromSec, int initValue, IntBinaryOperator op, int defaultValue) {
        long low = fromSec / unitSeconds;
        long high = low - (minutes * 60) / unitSeconds;

        Node node = head.get();
        while (node != null && node.t > low) {
            node = node.n;
        }

        int y = initValue;
        boolean exists = false;
        while (node != null && node.t > high) {
            int v = node.value.get();
            if (v != this.initValue) {
                y = op.applyAsInt(y, v);
                exists = true;
            }
            node = node.n;
        }

        if (exists) {
            return y;
        }
        else {
            return defaultValue;
        }
    }

    public enum StatisticMethod {
        STATMAX, STATMIN, STATSUM
    }

    private static final Map<StatisticMethod, Integer> initValues = ImmutableMap.of(
            StatisticMethod.STATSUM, 0,
            StatisticMethod.STATMAX, Integer.MIN_VALUE, StatisticMethod.STATMIN, Integer.MAX_VALUE);

    private static class Node {
        public Node(long tinkle, int value, Node next) {
            this.value = new AtomicInteger(value);
            this.t = tinkle;
            this.n = next;
        }

        public final AtomicInteger value;
        public final long t;
        public Node n;
    }

}
