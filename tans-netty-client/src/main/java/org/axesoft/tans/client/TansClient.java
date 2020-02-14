package org.axesoft.tans.client;

import io.netty.util.concurrent.Future;
import org.apache.commons.lang3.Range;


public interface TansClient {
    Future<Range<Long>> acquire(String key, int n, boolean ignoreLeader);
    void close();
}
