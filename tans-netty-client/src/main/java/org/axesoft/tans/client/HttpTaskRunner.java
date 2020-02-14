package org.axesoft.tans.client;

import com.google.common.util.concurrent.Monitor;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpTaskRunner {
    private final static AttributeKey<InetSocketAddress> ATTR_ADDRESS = AttributeKey.newInstance("ADDRESS");
    private final static AttributeKey<HttpClient> HTTP_CLIENT = AttributeKey.newInstance("HTTP_CLIENT");
    private final static AttributeKey<HttpTask> HTTP_TASK = AttributeKey.newInstance("HTTP_TASK");

    private static final Logger logger = LoggerFactory.getLogger(HttpTaskRunner.class);

    public interface ResponseHandler {
        void process(InetSocketAddress from, HttpResponse response, HttpContent content);
    }

    private static class HttpTask {
        InetSocketAddress address;
        HttpRequest request;
        int connectionCount;
        int requestCount;
        AtomicInteger finishedRequestCount;

        public HttpTask(InetSocketAddress address, HttpRequest request, int connectionCount, int requestCount) {
            this.address = address;
            this.request = request;
            this.connectionCount = connectionCount;
            this.requestCount = requestCount;
            this.finishedRequestCount = new AtomicInteger(0);
        }
    }

    public static boolean isRedirectCode(int code) {
        switch (code) {
            case 300:
            case 301:
            case 302:
            case 303:
            case 305:
            case 307:
                return true;
            default:
                return false;
        }
    }


    private NioEventLoopGroup worker;
    private ConcurrentMap<InetSocketAddress, HttpClient> clientMap;
    private final ResponseHandler responseHandler;
    private List<HttpTask> tasks = new CopyOnWriteArrayList<>();

    private final Monitor connectedMonitor = new Monitor();
    private final Monitor.Guard allConnectedGuard = new Monitor.Guard(connectedMonitor) {
        @Override
        public boolean isSatisfied() {
            int expectedConnectionCount = 0;
            for (HttpTask task : tasks) {
                expectedConnectionCount += task.connectionCount;
            }

            int actualConnectionCount = 0;
            for (HttpClient client : clientMap.values()) {
                actualConnectionCount += client.channels.size();
            }

            return actualConnectionCount >= expectedConnectionCount;
        }
    };


    public HttpTaskRunner(ResponseHandler handler) {
        this.responseHandler = handler;
    }

    public void addTask(String host, int port, HttpRequest request, int connectionNumber, int requestNumber) {
        tasks.add(new HttpTask(InetSocketAddress.createUnresolved(host, port), request, connectionNumber, requestNumber));
    }

    public void run() throws InterruptedException {
        worker = new NioEventLoopGroup();
        clientMap = new ConcurrentHashMap<>();

        for (HttpTask task : tasks) {
            HttpClient client = getOrCreateHttpClient(task.address);

            for (int i = 0; i < task.connectionCount; i++) {
                client.addOneConnection(task);
            }
        }


        boolean ok = connectedMonitor.enterWhen(allConnectedGuard, 30, TimeUnit.SECONDS);
        if (!ok) {
            logger.error("Can not create all connections in time, abort");
            this.worker.shutdownGracefully();
            return;
        }
        logger.info("Start send request");
        long start = System.nanoTime();
        try {
            for (HttpTask task : tasks) {
                HttpClient client = getOrCreateHttpClient(task.address);
                client.writeAndFlush(task, task.request);
            }
        }
        finally {
            connectedMonitor.leave();
        }

        for (int t = this.activeConnectionCount(), j = 0; t > 0; t = this.activeConnectionCount(), j++) {
            if (j % 50 == 0) {
                logger.info("Working connection count {}, finished request {}", t, this.finishedRequestCount());
            }
            Thread.sleep(100);
        }

        int n = this.finishedRequestCount();
        double millis = (System.nanoTime() - start) / 1e+6;
        double qps = n / (millis / 1000.0);
        System.out.println(String.format("POST %d in %.3f millis, QPS is %.0f", n, millis, qps));

        this.worker.shutdownGracefully();
    }


    public HttpClient getOrCreateHttpClient(InetSocketAddress address) {
        HttpClient client = clientMap.get(address);
        if (client == null) {
            synchronized (this.clientMap) {
                client = clientMap.get(address);
                if (client == null) {
                    client = new HttpClient(this.worker, address.getHostName(), address.getPort());
                    this.clientMap.put(address, client);
                }
            }
        }
        return client;
    }

    public int activeConnectionCount() {
        int actualConnectionCount = 0;
        for (HttpClient client : clientMap.values()) {
            actualConnectionCount += client.channels.size();
        }
        return actualConnectionCount;
    }

    private int finishedRequestCount(){
        int t = 0;
        for (HttpTask task : tasks) {
            t += task.finishedRequestCount.get();
        }
        return t;
    }
    private class HttpClient {
        private final String host;
        private final int port;

        private Bootstrap bootstrap;
        private ChannelGroup channels;

        private HttpClient(NioEventLoopGroup worker, String host, int port) {
            this.host = host;
            this.port = port;
            this.channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
            this.bootstrap = new Bootstrap();
            this.bootstrap.group(worker)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new HttpClientCodec());
                            // Remove the following line if you don't want automatic content decompression.
                            p.addLast(new HttpContentDecompressor());
                            // Uncomment the following line if you don't want to handle HttpContents.
                            //p.addLast(new HttpObjectAggregator(1048576));
                            p.addLast(new HttpClientHandler());
                        }
                    });
        }

        private ChannelFuture addOneConnection(HttpTask task) {
            ChannelFuture future = this.bootstrap.connect(host, port);
            future.addListener(f -> {
                if (f.isSuccess()) {
                    logger.info("connected to {}:{}", host, port);
                    Channel channel = ((ChannelFuture) f).channel();
                    channel.attr(ATTR_ADDRESS).set(InetSocketAddress.createUnresolved(host, port));
                    channel.attr(HTTP_CLIENT).set(this);
                    channel.attr(HTTP_TASK).set(task);
                    channels.add(channel);

                    connectedMonitor.enter();
                    connectedMonitor.leave();
                }
                else {
                    logger.error("failed to connect to {}:{}", host, port);
                }
            });
            return future;
        }

        void writeAndFlush(HttpTask task, Object obj) {
            channels.writeAndFlush(obj, channel -> channel.attr(HTTP_TASK).get() == task);
        }
    }


    private class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {
        HttpResponse response;


        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            Channel channel = ctx.channel();
            HttpClient client = channel.attr(HTTP_CLIENT).get();
            client.channels.remove(channel);

            logger.info("Channel to {}:{} closed", client.host, client.port);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            //System.out.println("read a message");
            if (msg instanceof HttpResponse) {
                response = (HttpResponse) msg;
                //System.err.println("STATUS: " + response.status());
            }
            if (msg instanceof HttpContent) {
                InetSocketAddress addr = ctx.channel().attr(ATTR_ADDRESS).get();
                HttpTask task = ctx.channel().attr(HTTP_TASK).get();

                HttpTaskRunner.this.responseHandler.process(addr, response, (HttpContent) msg);

                int i = task.finishedRequestCount.incrementAndGet();
                if (i >= task.requestCount) {
                    ctx.close();
                }
                else {
                    ctx.writeAndFlush(task.request);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}

