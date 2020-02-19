package org.axesoft.tans.domain;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class CounterTest {
    @Test
    public void test1() {
        String[] keys = new String[]{"pig.id", "star.id", "girl.id", "monkey.id", "actress.id", "object-id-1", "entity-id-2", "army49.division952.soldier9.id"};
        System.out.println(Math.abs(Integer.MIN_VALUE));
        for (String k : keys) {
            int c1 = k.hashCode();
            int c2 = (c1 < 0 ? -c1 : c1);
            System.out.println(String.format("key = %s, hashcode=%d, squad=%d", k, c2, c2 % 18));
        }
        assertTrue(1 == 1);
    }

    @Test
    public void testMask() throws Exception {
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
