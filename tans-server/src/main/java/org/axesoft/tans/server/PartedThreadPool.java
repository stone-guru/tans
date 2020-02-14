package org.axesoft.tans.server;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.axesoft.jaxos.RequestExecutor;
import org.axesoft.jaxos.base.SlideWindowMetric;

import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author bison
 * @sine 2020/1/16.
 */
public class PartedThreadPool implements RequestExecutor {
    private TansExecutor[] executors;

    public PartedThreadPool(int threadNum, String threadName) {
        this.executors = new TansExecutor[threadNum];
        for (int i = 0; i < threadNum; i++) {
            this.executors[i] = new TansExecutor(i, threadName + "-" + i);
        }
    }

    @Override
    public ListenableFuture<Void> submit(int i, Runnable r) {
        return this.executors[i].submit(() -> {
            r.run();
            return null;
        });
    }

    private int totalExecTimes() {
        int t = 0;
        for (TansExecutor executor : this.executors) {
            t += executor.totalExecTimes();
        }
        return t;
    }

    private int lastMinuteWaitingSize() {
        int t = 0;
        for (TansExecutor executor : this.executors) {
            t = Math.max(executor.lastMinuteWaitingSize(), t);
        }
        return t;
    }

    private static class TansExecutor {
        private ThreadPoolExecutor threadPool;
        private ListeningExecutorService executor;
        private SlideWindowMetric waitingTaskCounter;
        private AtomicInteger execTimes;

        private TansExecutor(int i, String name) {
            this.threadPool = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    (r) -> {
                        Thread thread = new Thread(r, name);
                        thread.setDaemon(true);
                        return thread;
                    });
            this.executor = MoreExecutors.listeningDecorator(this.threadPool);
            this.waitingTaskCounter = new SlideWindowMetric(1, SlideWindowMetric.StatisticMethod.STATMAX, 1);
            this.execTimes = new AtomicInteger(0);
        }

        private <T> ListenableFuture<T> submit(Callable<T> task) {
            ListenableFuture<T> f = this.executor.submit(task);
            this.execTimes.incrementAndGet();
            this.waitingTaskCounter.recordForPresent(this.threadPool.getQueue().size());
            return f;
        }

        private int totalExecTimes() {
            return this.execTimes.get();
        }

        private int lastMinuteWaitingSize() {
            return this.waitingTaskCounter.getMax(0);
        }
    }
}
