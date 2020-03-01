package org.axesoft.tans.client;

import org.apache.commons.lang3.Range;

import java.util.concurrent.CompletableFuture;


public interface TansClient {
    Range<Long> acquire(String key, int n, boolean ignoreLeader);
    double durationMillisPerRequest();
    void close();
}
