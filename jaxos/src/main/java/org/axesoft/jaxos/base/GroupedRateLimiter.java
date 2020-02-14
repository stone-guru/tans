package org.axesoft.jaxos.base;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author bison
 * @sine 2020/1/2.
 */
public class GroupedRateLimiter {
    private ConcurrentMap<Integer, RateLimiter> rateLimiterMap = new ConcurrentHashMap<>();
    private double permitsPerSecond;

    public GroupedRateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
    }

    public boolean tryAcquireFor(int id){
        RateLimiter limiter = rateLimiterMap.computeIfAbsent(id, k -> RateLimiter.create(this.permitsPerSecond));
        return limiter.tryAcquire();
    }

    public void execMaybe(int id, Runnable r){
        if(this.tryAcquireFor(id)){
            r.run();
        }
    }
}
