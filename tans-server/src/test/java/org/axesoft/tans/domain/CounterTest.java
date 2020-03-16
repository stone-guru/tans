package org.axesoft.tans.domain;

import com.google.common.primitives.Longs;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class CounterTest {
    @Test
    public void test1() {
        String[] keys = new String[]{"goose.id", "tiger.id", "lion05.id", "moose.id", "pig.id", "star15.id", "girl.id", "monkey.id", "actress.id", "soldier.id"};
        System.out.println(Math.abs(Integer.MIN_VALUE));
        for (String k : keys) {
            int c1 = k.hashCode();
            int c2 = (c1 < 0 ? -c1 : c1);
            System.out.println(String.format("key = %s, hashcode=%d, squad=%d", k, c2, c2 % 6));
        }
        assertTrue(1 == 1);
    }

    static void writeRawVarint32(int value) {
        while (true) {
            if ((value & ~0x7F) == 0) {
                System.out.println(String.format("%x", value));
                return;
            } else {
                int v = (value & 0x7F) | 0x80;
                byte b = (byte)v;
                System.out.print(String.format("%x,", b));
                value >>>= 7;
            }
        }
    }

    @Test
    public void testMask() throws Exception {
        System.out.println(Long.parseLong("ff", 16));
        writeRawVarint32(200);
        writeRawVarint32(300);
        writeRawVarint32(500);
        writeRawVarint32(1234);
        writeRawVarint32(8234);
        writeRawVarint32(23456);

        for (String s : new String[]{
                "$home/var", "$home_dir/var", "$homedir/abc"
        }) {
            System.out.println(s.replaceAll("\\$home\\b", System.getProperty("user.home")));
        }
    }

    @Test
    public void testFuture() {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        CompletableFuture<Integer> f2 = future.completeOnTimeout(2, 500, TimeUnit.MILLISECONDS)
                .thenApply(i -> i * 100);

        f2.thenAccept(i -> {
            System.out.println("Get value " + i);
        });

        new Thread(() -> {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                return;
            }
            System.out.println(future.isDone());
            System.out.println(f2.isDone());
            future.complete(9);
        }).run();

    }
}
