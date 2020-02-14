package org.axesoft.jaxos.algo;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.axesoft.jaxos.base.GroupedRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/9/17.
 */
public class EventWorkerPool implements EventTimer {
    private static Logger logger = LoggerFactory.getLogger(EventWorkerPool.class);

    private static final int THREAD_QUEUE_CAPACITY = 10 * 1024;
    private ExecutorService[] ballotExecutors;
    private Supplier<EventDispatcher> eventDispatcherSupplier;
    private HashedWheelTimer timer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS);
    private GroupedRateLimiter rateLimiter = new GroupedRateLimiter(1.0/10.0);
    private ExecutorService backendExecutor;

    public EventWorkerPool(int threadNum, Supplier<EventDispatcher> eventDispatcherSupplier) {
        this.eventDispatcherSupplier = eventDispatcherSupplier;
        this.ballotExecutors = new ExecutorService[threadNum];
        for (int i = 0; i < threadNum; i++) {
            final int threadNo = i;
            this.ballotExecutors[i] = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(THREAD_QUEUE_CAPACITY),
                    (r) -> {
                        String name = "EventWorkerThread-" + threadNo;
                        Thread thread = new Thread(r, name);
                        thread.setDaemon(true);
                        return thread;
                    });
        }

        this.backendExecutor = new ThreadPoolExecutor(2, 2,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(THREAD_QUEUE_CAPACITY),
                (r) -> {
                    String name = "EventWorkerBackendThread";
                    Thread thread = new Thread(r, name);
                    thread.setDaemon(true);
                    return thread;
                });
    }

    public void submitBackendTask(Runnable r){
        this.backendExecutor.submit(r);
    }

    public void queueTask(int squadId, Runnable r) {
        int n = squadId % ballotExecutors.length;
        try {
            this.ballotExecutors[n].submit(new RunnableWithLog(squadId, logger, r));
        }
        catch (RejectedExecutionException e) {
            this.rateLimiter.execMaybe(n, () -> logger.warn("S{} task rejected by executor {}", squadId, n));
        }
    }

    public void submitEventToSelf(Event event) {
        this.submitEvent(event, this::submitEventToSelf);
    }

    public void submitEvent(Event event, Consumer<Event> resultConsumer) {
        queueTask(event.squadId(),
                () -> {
                    Event result = this.eventDispatcherSupplier.get().processEvent(event);
                    if (result != null) {
                        resultConsumer.accept(result);
                    }
                });
    }

    public void directCallSelf(Event event) {
        this.eventDispatcherSupplier.get().processEvent(event);
    }

    @Override
    public Timeout createTimeout(long delay, TimeUnit timeUnit, Event timeoutEvent) {
        return timer.newTimeout((t) -> this.submitEventToSelf(timeoutEvent), delay, timeUnit);
    }

    public void shutdown() {
        for (ExecutorService executor : this.ballotExecutors) {
            executor.shutdownNow();
        }
    }
}
