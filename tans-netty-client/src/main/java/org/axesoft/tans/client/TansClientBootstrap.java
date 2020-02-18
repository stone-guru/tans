package org.axesoft.tans.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.FailedFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.BufferOverflowException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

public class TansClientBootstrap {
    private final static AttributeKey<InetSocketAddress> ATTR_ADDRESS = AttributeKey.newInstance("ADDRESS");
    private final static AttributeKey<HttpConnector> ATTR_CONNECTOR = AttributeKey.newInstance("HTTP_CLIENT");
    private final static String TANS_HANDLER_NAME = "tansHandler";

    private static final Logger logger = LoggerFactory.getLogger(TansClientBootstrap.class);

    private static class AcquireRequest {
        final String key;
        final long n;
        final boolean ignoreLeader;

        public AcquireRequest(String key, long n, boolean ignoreLeader) {
            this.key = key;
            this.n = n;
            this.ignoreLeader = ignoreLeader;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AcquireRequest that = (AcquireRequest) o;

            if (n != that.n) return false;
            if (ignoreLeader != that.ignoreLeader) return false;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            int result = key.hashCode();
            result = 31 * result + (int) (n ^ (n >>> 32));
            result = 31 * result + (ignoreLeader ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "AcquireRequest{" +
                    "key='" + key + '\'' +
                    ", n=" + n +
                    ", ignoreLeader=" + ignoreLeader +
                    '}';
        }
    }


    private static class TansResult {
        private final Range<Long> range;
        private final String redirectAddress;

        private TansResult(Range<Long> range) {
            this.range = range;
            this.redirectAddress = null;
        }

        private TansResult(String redirectAddress) {
            this.redirectAddress = redirectAddress;
            this.range = null;
        }

        private boolean hasResult(){
            return this.range != null;
        }
    }

    private class HttpConnector {
        private final String host;
        private final int port;

        private Bootstrap bootstrap;
        private volatile Channel channel = null;

        private HttpConnector(NioEventLoopGroup worker, String host, int port) {
            this.host = host;
            this.port = port;
            this.bootstrap = new Bootstrap();
            this.bootstrap.group(worker)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .remoteAddress(host, port)
                    .handler(new LoggingHandler(LogLevel.ERROR))
                    .handler(new ChannelInitializer<>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            logger.info("Channel to {}:{} created", host, port);
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new HttpClientCodec());
                            //p.addLast(new HttpObjectAggregator(1048576));
                            p.addLast(TANS_HANDLER_NAME, new TansClientHandler());

                            ch.attr(ATTR_CONNECTOR).set(HttpConnector.this);
                            ch.attr(ATTR_ADDRESS).set(InetSocketAddress.createUnresolved(host, port));
                        }
                    });

            connect();
        }

        private void connect() {
            if (closed) {
                return;
            }
            this.bootstrap.connect(host, port).addListener(f -> {
                if (f.isSuccess()) {
                    logger.info("Connected to {}:{}", host, port);
                    HttpConnector.this.channel = ((ChannelFuture) f).channel();
                }
                else {
                    logger.error("Unable to connect to {}:{}, try again", host, port);
                    worker.schedule(() -> connect(), 1, TimeUnit.SECONDS);
                }
            });
        }

        private Channel getChannel() {
            return this.channel;
        }
    }

    private class TansClientHandler extends SimpleChannelInboundHandler<HttpObject> {
        private BlockingQueue<Promise<TansResult>> promises = new ArrayBlockingQueue<>(8);
        private ChannelHandlerContext ctx;
        private HttpRequest request;
        private int times = 0;
        private HttpResponse response;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            this.ctx = ctx;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);

            synchronized (this) {
                Promise<TansResult> promise;
                while ((promise = this.promises.poll()) != null) {
                    promise.setFailure(new IOException("Connection lost"));
                }
                this.promises = null;
                Channel channel = ctx.channel();
                HttpConnector client = channel.attr(ATTR_CONNECTOR).get();
                client.connect();
                logger.info("Channel to {}:{} closed", client.host, client.port);
            }
        }

        @Override
        public synchronized void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (this.promises == null) {
                logger.info("Handler closed, abandon response");
                return;
            }
            if (msg instanceof HttpResponse) {
                response = (HttpResponse) msg;
            }

            if (msg instanceof HttpContent) {
                times++;
                HttpContent content = (HttpContent) msg;
                String body = content.content().toString(CharsetUtil.UTF_8);

                if (response.status().code() == 200) {
                    Range<Long> r;
                    try {
                        String s = firstLine(body);
                        String[] rx = s.split(",");
                        r = Range.between(Long.parseLong(rx[1]), Long.parseLong(rx[2]));
                        logger.debug("Got result {}", r);
                    }
                    catch (Exception e) {
                        promises.poll().setFailure(e);
                        return;
                    }
                    promises.poll().setSuccess(new TansResult(r));
                }
                else if (isRedirectCode(response.status().code())) {
                    String location = response.headers().get(HttpHeaderNames.LOCATION);
                    promises.poll().setSuccess(new TansResult(location));
                }
                else {
                    if (response.status().code() == 409 && times < 3) {
                        logger.debug("Retry send request {}", times);
                        ctx.writeAndFlush(request);
                        return;
                    }
                    Promise<TansResult> p = promises.poll();
                    if(p != null) {
                        Exception e = new Exception(response.status().toString());
                        p.setFailure(e);
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }


        public synchronized io.netty.util.concurrent.Future<TansResult> acquire(AcquireRequest req) {
            times = 0;
            if (this.promises == null) {
                return this.ctx.executor().newFailedFuture(new IllegalStateException("Channel closed"));
            }

            try {
                request = requestCache.get(req);
            }
            catch (ExecutionException e) {
                return this.ctx.executor().newFailedFuture(e);
            }

            Promise<TansResult> p = this.ctx.executor().newPromise();
            if (promises.offer(p)) {
                ctx.writeAndFlush(request);
            }
            else {
                p.setFailure(new BufferOverflowException());
            }

            return p;
        }
    }

    private class PooledTansClient implements TansClient {
        private Function<String, HttpConnector> connectorFunc;
        private HttpConnector connector;

        public PooledTansClient(Function<String, HttpConnector> connectorFunc) {
            this.connectorFunc = connectorFunc;
        }

        private synchronized HttpConnector getConnector(String key) {
            connector = this.connectorFunc.apply(key);
            return connector;
        }

        private synchronized HttpConnector redirect(String key, String location) {
            if(logger.isDebugEnabled()) {
                logger.debug("Handler redirect of {} for {}", location, key);
            }
            URI uri;
            try {
                uri = new URI(location);
            }
            catch (URISyntaxException e) {
                logger.error("error", e);
                return null;
            }

            InetSocketAddress address = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
            connector = getOrCreateHttpClient(address);
            keyLeaderCache.put(key, address);
            return connector;
        }

        @Override
        public Future<Range<Long>> acquire(String key, int n, boolean ignoreLeader) {
            HttpConnector connector = getConnector(key);
            if (connector == null) {
                return new FailedFuture<>(worker.next(), new ConnectException("Can not get connection for " + key));
            }
            Channel c = connector.getChannel();
            if (c == null) {
                return new FailedFuture<>(worker.next(), new ConnectException("Can not get channel for " + key));
            }

            final Promise<Range<Long>> promise = new DefaultPromise<>(worker.next());
            acquire(new AcquireRequest(key, n, ignoreLeader), c, promise);
            return promise;
        }

        private void acquire(AcquireRequest req, Channel channel, Promise<Range<Long>> promise) {
            TansClientHandler handler = (TansClientHandler) channel.pipeline().get(TANS_HANDLER_NAME);
            handler.acquire(req).addListener(f -> {
                if (f.isSuccess()) {
                    @SuppressWarnings("unchecked")
                    TansResult r = ((Future<TansResult>) f).get();
                    if (r.hasResult()) {
                        promise.setSuccess(r.range);
                    }
                    else {
                        HttpConnector c2 = redirect(req.key, r.redirectAddress);
                        worker.submit(() -> acquire(req, c2.getChannel(), promise));
                    }
                }
                else {
                    String msg = f.cause().getMessage();
                    if (msg.contains("409")) {//conflict
                        logger.info("Request {} encounter CONFLICT retry after 100 ms", req);
                        worker.schedule(() -> acquire(req, channel, promise), 100, TimeUnit.MILLISECONDS);
                    }
                    else if (msg.contains("500")) {
                        logger.info("encounter INTERNAL error retry after 300 ms");
                        worker.schedule(() -> acquire(req, channel, promise), 300, TimeUnit.MILLISECONDS);
                    }
                    else {
                        promise.setFailure(f.cause());
                    }
                }
            });
        }

        @Override
        public synchronized void close() {
        }
    }

    private ConcurrentMap<InetSocketAddress, HttpConnector> connectorMap;
    private NioEventLoopGroup worker;

    private LoadingCache<AcquireRequest, HttpRequest> requestCache;
    private LoadingCache<String, InetSocketAddress> keyLeaderCache;
    private volatile boolean closed = false;

    public TansClientBootstrap(String servers) {
        worker = new NioEventLoopGroup(8);

        connectorMap = new ConcurrentHashMap<>();
        for (InetSocketAddress addr : parseAddresses(servers)) {
            getOrCreateHttpClient(addr);
        }

        requestCache = CacheBuilder.newBuilder()
                .concurrencyLevel(8)
                .expireAfterAccess(30, TimeUnit.SECONDS)
                .build(new CacheLoader<AcquireRequest, HttpRequest>() {
                    @Override
                    public HttpRequest load(AcquireRequest req) throws Exception {
                        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                                String.format("/acquire?key=%s&n=%d&ignoreleader=%s", req.key, req.n, Boolean.toString(req.ignoreLeader)),
                                Unpooled.EMPTY_BUFFER);
                        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                        request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.TEXT_PLAIN);

                        return request;
                    }
                });

        keyLeaderCache = CacheBuilder.newBuilder()
                .concurrencyLevel(8)
                .expireAfterAccess(Duration.ofMinutes(10))
                .build(new CacheLoader<String, InetSocketAddress>() {
                    @Override
                    public InetSocketAddress load(String key) throws Exception {
                        int randInt = (int) (Math.random() * 10000);
                        Iterator<InetSocketAddress> it = connectorMap.keySet().iterator();
                        int k = randInt % connectorMap.size();
                        InetSocketAddress result = it.next();
                        for (int i = 0; i < k; i++) {
                            result = it.next();
                        }
                        return result;
                    }
                });
    }

    public TansClient getClient() {
        return new PooledTansClient(this::selectServer);
    }

    public void close() {
        this.closed = true;
        this.worker.shutdownGracefully();
    }

    private HttpConnector selectServer(String key) {
        try {
            InetSocketAddress addr = keyLeaderCache.get(key);
            return connectorMap.get(addr);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private List<InetSocketAddress> parseAddresses(String servers) {
        ImmutableList.Builder<InetSocketAddress> builder = ImmutableList.builder();
        String[] sx = servers.split(";");
        for (String s : sx) {
            String[] ax = s.split(":");
            if (ax == null || ax.length != 2) {
                throw new IllegalArgumentException(s + " is not a valid server address like 'address:port'");
            }

            int port = 0;
            try {
                port = Integer.parseInt(ax[1]);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port number in '" + s + "'");
            }

            builder.add(InetSocketAddress.createUnresolved(ax[0], port));
        }
        return builder.build();
    }

    private HttpConnector getOrCreateHttpClient(InetSocketAddress address) {
        return connectorMap.compute(address, (k, c) -> {
            if (c != null) {
                return c;
            }
            return new HttpConnector(this.worker, k.getHostName(), k.getPort());
        });
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

    public static String firstLine(String s) {
        int index = s.indexOf('\r');
        if (index < 0) {
            return s;
        }
        return s.substring(0, index);
    }
}
