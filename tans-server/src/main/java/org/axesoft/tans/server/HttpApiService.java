package org.axesoft.tans.server;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.base.Either;
import org.axesoft.jaxos.base.LongRange;
import org.axesoft.jaxos.base.NumberedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * An HTTP server that sends back the content of the received HTTP request
 * in a pretty plaintext form.
 */
public class HttpApiService extends AbstractExecutionThreadService {
    private static final Logger logger = LoggerFactory.getLogger(HttpApiService.class);

    private static final String SERVICE_NAME = "Take-A-Number System";

    private static final String TOKEN_HEADER_NAME = "X-tans-token";

    private static final ByteBuf NOT_FOUND_BYTEBUF = Unpooled.copiedBuffer("Not Found\n", CharsetUtil.UTF_8);

    private TansService tansService;
    private TansConfig config;

    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private TansAcquireRequestQueue[] acquireRequestQueues;
    private Timer queueTimer;

    private TansMetrics metrics;

    private Map<String, TansHttpRequestHandler> handlerMap;

    private static class TansAcquireRequest {
        final KeyLong keyLong;
        final boolean keepAlive;
        final boolean ignoreLeader;
        final ChannelHandlerContext ctx;
        final long timestamp;

        public TansAcquireRequest(String key, long v, boolean ignoreLeader, boolean keepAlive, ChannelHandlerContext ctx) {
            this.keyLong = new KeyLong(key, v);
            this.keepAlive = keepAlive;
            this.ignoreLeader = ignoreLeader;
            this.ctx = ctx;
            this.timestamp = System.currentTimeMillis();
        }
    }

    interface TansHttpRequestHandler {
        void process(String[] path, HttpRequest request, ChannelHandlerContext context);

        FullHttpResponse createAcquireResponseForSuccess(TansAcquireRequest request, LongRange result);

        FullHttpResponse createAcquireResponseForRedirect(TansAcquireRequest request, int otherLeaderId);

        FullHttpResponse createAcquireResponseForError(TansAcquireRequest request, String msg);
    }


    public HttpApiService(TansConfig config, TansService tansService) {
        this.tansService = tansService;
        this.config = config;
        this.metrics = new TansMetrics(config.serverId(), this.config.jaxConfig().partitionNumber(), this.tansService::keyCountOf);

        this.acquireRequestQueues = new TansAcquireRequestQueue[config.jaxConfig().partitionNumber()];
        for (int i = 0; i < acquireRequestQueues.length; i++) {
            this.acquireRequestQueues[i] = new TansAcquireRequestQueue(i, config.requestBatchSize());
        }

        this.handlerMap = ImmutableMap.of( "v0", new TansHttpRequestHandlerV1(),"v1", new TansHttpRequestHandlerV2());

        super.addListener(new Listener() {
            @Override
            public void running() {
                logger.info("{} started at http://{}:{}/", SERVICE_NAME, config.address(), config.httpPort());
            }

            @Override
            public void stopping(State from) {
                logger.info("{} stopping HTTP API server", SERVICE_NAME);
            }

            @Override
            public void terminated(State from) {
                logger.info("{} terminated from state {}", SERVICE_NAME, from);
            }

            @Override
            public void failed(State from, Throwable failure) {
                logger.error(String.format("%s failed %s due to exception", SERVICE_NAME, from), failure);
            }
        }, MoreExecutors.directExecutor());
    }

    @Override
    protected void run() throws Exception {
        this.queueTimer = new Timer("Queue Flush", true);
        this.queueTimer.scheduleAtFixedRate(new TimerTask() {
            private boolean interrupted = false;

            @Override
            public void run() {
                if (interrupted) {
                    return;
                }
                long current = System.currentTimeMillis();
                for (TansAcquireRequestQueue q : acquireRequestQueues) {
                    try {
                        q.click(current);
                    }
                    catch (Exception e) {
                        if (e.getCause() instanceof InterruptedException) {
                            interrupted = true;
                        }
                        else {
                            logger.error("Exception when flush request queue", e);
                        }
                    }
                }
            }
        }, 1000, 1);

        this.bossGroup = new NioEventLoopGroup(config.nettyBossThreadNumber(), new NumberedThreadFactory("NettyTansApiBoss"));
        this.workerGroup = new NioEventLoopGroup(config.nettyWorkerThreadNumber(), new NumberedThreadFactory("NettyTansApiWorker"));

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.ERROR))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new HttpRequestDecoder());
                            p.addLast(new HttpResponseEncoder());
                            p.addLast(new TansHttpChannelHandler());
                        }
                    });

            this.serverChannel = b.bind(config.httpPort()).sync().channel();

            this.serverChannel.closeFuture().sync();
        }
        finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    protected void triggerShutdown() {
        if (this.serverChannel != null) {
            this.serverChannel.close();
        }
    }

    private static FullHttpResponse createResponse(ChannelHandlerContext ctx, HttpResponseStatus status, String... vx) {
        ByteBuf buf;
        if (vx == null || vx.length == 0) {
            buf = Unpooled.copiedBuffer("\r\n", CharsetUtil.UTF_8);
        }
        else {
            buf = ctx.alloc().directBuffer();
            for (String s : vx) {
                buf.writeCharSequence(s, CharsetUtil.UTF_8);
            }
        }
        FullHttpResponse result = new DefaultFullHttpResponse(HTTP_1_1, status, buf);
        return result;
    }


    private static String getRawPath(String s) {
        int i = s.indexOf("?");
        if (i < 0) {
            return s;
        }
        else {
            return s.substring(0, i);
        }
    }


    private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, FullHttpResponse response) {
        if(!response.headers().contains(HttpHeaderNames.CONTENT_TYPE)){
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        }
        response.headers().set(HttpHeaderNames.HOST, config.address());
        response.headers().set(HttpHeaderNames.FROM, Integer.toString(config.httpPort()));
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }

        ChannelFuture future = ctx.writeAndFlush(response);
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private TansHttpRequestHandler getRequestHandler(String version) {
        return this.handlerMap.get(version);
    }

    public class TansHttpChannelHandler extends SimpleChannelInboundHandler {
        private HttpRequest request;

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                this.request = (HttpRequest) msg;
                if (HttpUtil.is100ContinueExpected(request)) {
                    send100Continue(ctx);
                }
            }

            if (msg instanceof LastHttpContent) {
                if (!verifyToken(ctx, request)) {
                    return;
                }

                String path = getRawPath(request.uri());
                Pair<String, String[]> p = extractPath(path);
                TansHttpRequestHandler handler = getRequestHandler(p.getLeft());
                if (handler == null) {
                    writeResponse(ctx, false,
                            new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                                    NOT_FOUND_BYTEBUF.retainedDuplicate()));
                }
                else {
                    handler.process(p.getRight(), request, ctx);
                }
            }
        }

        private boolean verifyToken(ChannelHandlerContext ctx, HttpRequest request) {
            if(request.uri().startsWith("/metrics")){
                return true;
            }

            if (Strings.isNullOrEmpty(config.httpToken())) {
                return true;
            }

            String token = request.headers().get(TOKEN_HEADER_NAME);
            if (!config.httpToken().equals(token)) {
                writeResponse(ctx, false,
                        new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.FORBIDDEN,
                                Unpooled.EMPTY_BUFFER.retainedDuplicate()));
                return false;
            }
            return true;
        }

        private final String[] EMPTY_STR_ARRAY = new String[0];

        public Pair<String, String[]> extractPath(String url) {

            String sx[] = StringUtils.split(url.substring(1), '/');
            if (sx == null || sx.length == 0) {
                return Pair.of("v0", EMPTY_STR_ARRAY);
            }
            else if (sx[0].equals("v0")) {
                return Pair.of("v0", cdr(sx));
            }
            else if (sx[0].equals("v1")) {
                return Pair.of("v1", cdr(sx));
            }
            else {
                return Pair.of("v0", sx);
            }
        }

        private String[] cdr(String[] ax) {
            String[] bx = new String[ax.length - 1];
            System.arraycopy(ax, 1, bx, 0, ax.length - 1);
            return bx;
        }

        private void send100Continue(ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER.retainedDuplicate());
            ctx.write(response);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!"Connection reset by peer".equals(cause.getMessage())) {
                logger.warn("Server channel got exception", cause);
            }
            ctx.close();
        }
    }

    private class TansHttpRequestHandlerV1 implements TansHttpRequestHandler {

        //The v0 service URL is http://<host>:<port>/v0/acquire?key=<keyname>[&n=<number>][&ignoreleader=true|false]
        private static final String ACQUIRE_PATH = "acquire";
        private static final String KEY_ARG_NAME = "key";
        private static final String NUM_ARG_NAME = "n";
        private static final String IGNORE_LEADER_ARG_NAME = "ignoreleader";

        private static final String PARTITION_PATH = "partition_number";
        private static final String METRICS_PATH = "metrics";

        private static final String ARG_KEY_REQUIRED_MSG = "argument of '" + KEY_ARG_NAME + "' is required";

        @Override
        public void process(String pathSegments[], HttpRequest request, ChannelHandlerContext ctx) {
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            String path = pathSegments.length == 1 ? pathSegments[0] : "";

            if (ACQUIRE_PATH.equals(path)) {
                Either<String, TansAcquireRequest> either = parseAcquireRequest(request, keepAlive, ctx);
                if (!either.isRight()) {
                    FullHttpResponse r = HttpApiService.createResponse(ctx, HttpResponseStatus.BAD_REQUEST, either.getLeft());
                    writeResponse(ctx, keepAlive, r);
                    return;
                }

                TansAcquireRequest tansAcquireRequest = either.getRight();
                if (logger.isTraceEnabled()) {
                    logger.trace("Got request {}", tansAcquireRequest.keyLong);
                }
                int i = tansService.squadIdOf(tansAcquireRequest.keyLong.key());
                acquireRequestQueues[i].addRequest(this, tansAcquireRequest);
            }
            else if (METRICS_PATH.equals(path)) {
                String s1 = tansService.formatMetrics();
                String s2 = metrics.format();
                FullHttpResponse r = HttpApiService.createResponse(ctx, HttpResponseStatus.OK, s1, s2);
                writeResponse(ctx, false, r);
            }
            else if (PARTITION_PATH.equals(path)) {
                String s = Integer.toString(config.jaxConfig().partitionNumber());
                FullHttpResponse r = HttpApiService.createResponse(ctx, HttpResponseStatus.OK, s);
                writeResponse(ctx, keepAlive, r);
            }
            else {
                FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                        NOT_FOUND_BYTEBUF.retainedDuplicate());
                writeResponse(ctx, keepAlive, response);
            }
        }


        private Either<String, TansAcquireRequest> parseAcquireRequest(HttpRequest request, boolean keepAlive, ChannelHandlerContext ctx) {
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
            Map<String, List<String>> params = queryStringDecoder.parameters();

            List<String> vx = params.get(KEY_ARG_NAME);
            if (vx == null || vx.size() == 0) {
                return Either.left(ARG_KEY_REQUIRED_MSG);
            }
            String key = vx.get(0);
            if (Strings.isNullOrEmpty(key)) {
                return Either.left(ARG_KEY_REQUIRED_MSG);
            }

            long n = 1;
            List<String> nx = params.get(NUM_ARG_NAME);
            if (nx != null && nx.size() > 0) {
                try {
                    n = Long.parseLong(nx.get(0));
                    if (n <= 0) {
                        return Either.left("Zero or negative n '" + n + "'");
                    }
                }
                catch (NumberFormatException e) {
                    return Either.left("Invalid integer '" + nx.get(0) + "'");
                }
            }

            boolean ignoreLeader = false;
            List<String> bx = params.get(IGNORE_LEADER_ARG_NAME);
            if (bx != null && bx.size() > 0) {
                String flag = bx.get(0);
                if ("true".equalsIgnoreCase(flag)) {
                    ignoreLeader = true;
                }
                else if ("false".equalsIgnoreCase(flag)) {
                    ignoreLeader = false;
                }
                else {
                    return Either.left("Invalid ignore leader boolean value '" + flag + "'");
                }
            }

            return Either.right(new TansAcquireRequest(key, n, ignoreLeader, keepAlive, ctx));
        }

        @Override
        public FullHttpResponse createAcquireResponseForSuccess(TansAcquireRequest request, LongRange result) {
            if (logger.isTraceEnabled()) {
                logger.trace("Write response {},{},{},{}", request.keyLong.key(), request.keyLong.value(),
                        result.low(), result.high());
            }
            return createResponse(request.ctx, HttpResponseStatus.OK,
                    request.keyLong.key(), ",", Long.toString(result.low()), ",", Long.toString(result.high()), "\r\n");
        }

        @Override
        public FullHttpResponse createAcquireResponseForRedirect(TansAcquireRequest request, int otherLeaderId) {
            int httpPort = config.getPeerHttpPort(otherLeaderId);
            JaxosSettings.Peer peer = config.jaxConfig().getPeer(otherLeaderId);
            String url = String.format("http://v0/%s:%s%s?%s=%s&%s=%d",
                    peer.address(), httpPort, TansHttpRequestHandlerV1.ACQUIRE_PATH,
                    TansHttpRequestHandlerV1.KEY_ARG_NAME, request.keyLong.key(),
                    TansHttpRequestHandlerV1.NUM_ARG_NAME, request.keyLong.value());

            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, HttpResponseStatus.TEMPORARY_REDIRECT);
            response.headers().set(HttpHeaderNames.LOCATION, url);
            return response;
        }

        @Override
        public FullHttpResponse createAcquireResponseForError(TansAcquireRequest request, String msg) {
            FullHttpResponse response = createResponse(request.ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, msg, "\r\n");
            return response;
        }
    }

    private class TansHttpRequestHandlerV2 implements TansHttpRequestHandler {
        //The v2 service URL is http://<host>:<port>/v2/keys/<keyname>[?number=<number>][&ignoreleader=true|false]
        //PUT for acquiring an id range, GET for read the key value at present

        private static final String KEYS_ROOT_PATH = "keys";
        private static final String NUMBER_ARG_NAME = "number";


        private final String NOT_FOUND_MESSAGE = "{\"message\":\"Not Found\"}\n";
        @Override
        public void process(String[] pathSegments, HttpRequest request, ChannelHandlerContext context) {
            String p1 = pathSegments.length > 0 ? pathSegments[0] : "";
            String p2 = pathSegments.length > 1 ? pathSegments[1] : "";

            if (pathSegments.length == 2 && p1.equals(KEYS_ROOT_PATH)) {
                if (request.method().name().equals("PUT")) {
                    Either<String, TansAcquireRequest> either = parseAcquireRequest(p2, request, context);
                    if (either.isRight()) {
                        TansAcquireRequest tansAcquireRequest = either.getRight();
                        if (logger.isTraceEnabled()) {
                            logger.trace("Got request {}", tansAcquireRequest.keyLong);
                        }
                        int i = tansService.squadIdOf(tansAcquireRequest.keyLong.key());
                        acquireRequestQueues[i].addRequest(this, tansAcquireRequest);
                        return;
                    }
                }
                else if (request.method().name().equals("GET")) {
                    if (processKeyRead(p2, HttpUtil.isKeepAlive(request), context)) {
                        return;
                    }
                }
            }

            FullHttpResponse response = this.createJsonResponse(context, HttpResponseStatus.NOT_FOUND, NOT_FOUND_MESSAGE);
            writeResponse(context, false, response);
        }

        @Override
        public FullHttpResponse createAcquireResponseForSuccess(TansAcquireRequest request, LongRange result) {
            if (logger.isTraceEnabled()) {
                logger.trace("Write response {},{},{},{}", request.keyLong.key(), request.keyLong.value(),
                        result.low(), result.high());
            }
            FullHttpResponse response = this.createJsonResponse(request.ctx, HttpResponseStatus.OK,
                    "{\n",
                    "\"key\": ", "\"", request.keyLong.key(), "\",\n",
                    "\"applied\": ", Long.toString(request.keyLong.value()), ",\n",
                    "\"range\":  [", Long.toString(result.low()), ",", Long.toString(result.high()), "]\n",
                    "}\n");
            return response;
        }

        @Override
        public FullHttpResponse createAcquireResponseForRedirect(TansAcquireRequest request, int otherLeaderId) {
            int httpPort = config.getPeerHttpPort(otherLeaderId);
            JaxosSettings.Peer peer = config.jaxConfig().getPeer(otherLeaderId);
            String url = String.format("http://%s:%s/v1/keys/%s?%s=%d",
                    peer.address(), httpPort, request.keyLong.key(),
                    TansHttpRequestHandlerV1.NUM_ARG_NAME, request.keyLong.value());

            FullHttpResponse response = this.createJsonResponse(request.ctx, HttpResponseStatus.TEMPORARY_REDIRECT);
            response.headers().set(HttpHeaderNames.LOCATION, url);
            return response;
        }

        @Override
        public FullHttpResponse createAcquireResponseForError(TansAcquireRequest request, String msg) {
            FullHttpResponse response = createResponse(request.ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, msg,
                    "{\n",
                    "\"message\": \"", msg, "\"\n",
                    "}\n");
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
            return response;
        }

        private Either<String, TansAcquireRequest> parseAcquireRequest(String p2, HttpRequest request, ChannelHandlerContext ctx) {
            if (Strings.isNullOrEmpty(p2)) {
                return Either.left("key name not provide");
            }

            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
            Map<String, List<String>> params = queryStringDecoder.parameters();
            long n = 1;
            List<String> nx = params.get(NUMBER_ARG_NAME);
            if (nx != null && nx.size() > 0) {
                try {
                    n = Long.parseLong(nx.get(0));
                    if (n <= 0) {
                        return Either.left("Zero or negative n '" + n + "'");
                    }
                }
                catch (NumberFormatException e) {
                    return Either.left("Invalid integer '" + nx.get(0) + "'");
                }
            }

            return Either.right(new TansAcquireRequest(p2, n, false, HttpUtil.isKeepAlive(request), ctx));
        }

        private boolean processKeyRead(String keyName, boolean keepAlive, ChannelHandlerContext context) {
            Optional<TansNumber> opt = tansService.localValueOf(keyName);
            if (opt.isPresent()) {
                TansNumber n = opt.get();
                FullHttpResponse response = this.createJsonResponse(context, HttpResponseStatus.OK,
                        "{\n",
                        "\"key\": ", "\"", n.name(), "\",\n",
                        "\"value\": ", Long.toString(n.value()), ",\n",
                        "\"version\":  ", Long.toString(n.version()), ",\n",
                        "\"lastModified\": \"", new Date(n.timestamp()).toString(), "\"\n",
                        "}\r\n");
                writeResponse(context, keepAlive, response);
                return true;
            }
            return false;
        }

        private FullHttpResponse createJsonResponse(ChannelHandlerContext ctx, HttpResponseStatus status, String... vx) {
            FullHttpResponse r = HttpApiService.createResponse(ctx, status, vx);
            r.headers().add(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
            return r;
        }
    }

    private class TansAcquireRequestQueue {
        private final int squadId;
        private final int waitingSize;

        private long t0;
        private List<Pair<TansHttpRequestHandler, TansAcquireRequest>> waitingTasks;
        private RateLimiter logRateLimiter;

        public TansAcquireRequestQueue(int squadId, int waitingSize) {
            this.squadId = squadId;
            this.waitingSize = waitingSize;
            this.t0 = 0;
            this.waitingTasks = new ArrayList<>(waitingSize);
            this.logRateLimiter = RateLimiter.create(1.0 / 2.0);
        }

        private synchronized void addRequest(TansHttpRequestHandler handler, TansAcquireRequest request) {
            if (logger.isTraceEnabled()) {
                logger.trace("S{} RequestQueue accept request {}", squadId, request);
            }
            HttpApiService.this.metrics.incRequestCount();

            this.waitingTasks.add(Pair.of(handler, request));
            if (this.waitingTasks.size() == 1) {
                this.t0 = System.currentTimeMillis();
            }

            if (waitingTasks.size() >= waitingSize) {
                processRequests();
            }
        }

        private synchronized void click(long current) {
            if (t0 > 0 && current - t0 >= 2) { //2 ms
                logger.trace("S{}: Queue processEvent waiting tasks when timeout", squadId);
                processRequests();
            }
        }

        private void processRequests() {
            final List<Pair<TansHttpRequestHandler, TansAcquireRequest>> todo = ImmutableList.copyOf(this.waitingTasks);
            this.waitingTasks.clear();
            this.t0 = 0;

            workerGroup.submit(() -> process(todo));
        }

        private void process(List<Pair<TansHttpRequestHandler, TansAcquireRequest>> tasks) {
            boolean ignoreLeader = false;
            List<KeyLong> kvx = new ArrayList<>(tasks.size());
            for (Pair<TansHttpRequestHandler, TansAcquireRequest> p : tasks) {
                TansAcquireRequest req = p.getRight();
                kvx.add(req.keyLong);
                if (req.ignoreLeader) {
                    ignoreLeader = true;
                }
            }

            tansService.acquire(squadId, kvx, ignoreLeader)
                    .thenAcceptAsync(r -> {
                        final long now = System.currentTimeMillis();
                        switch (r.code()) {
                            case SUCCESS:
                                onProcessSuccess(tasks, r.value(), now);
                                break;
                            case REDIRECT:
                                HttpApiService.this.metrics.incRedirectCounter();
                                sendRedirect(tasks, r.redirectServerId());
                                break;
                            case ERROR:
                                onProcessFailure(tasks, r.errorMessage());
                                break;
                            default:
                                if (logRateLimiter.tryAcquire()) {
                                    logger.error("Un processed acquire result " + r.code());
                                }
                        }
                    }, workerGroup);
        }

        public void onProcessSuccess(List<Pair<TansHttpRequestHandler, TansAcquireRequest>> tasks, List<LongRange> result, long now) {
            for (int i = 0; i < tasks.size(); i++) {
                Pair<TansHttpRequestHandler, TansAcquireRequest> p = tasks.get(i);
                TansAcquireRequest request = p.getRight();
                TansHttpRequestHandler handler = p.getLeft();

                LongRange r = result.get(i);
                FullHttpResponse response = handler.createAcquireResponseForSuccess(request, r);

                writeResponse(request.ctx, request.keepAlive, response);

                HttpApiService.this.metrics.recordRequestElapsed(now - request.timestamp);
            }
        }

        public void onProcessFailure(List<Pair<TansHttpRequestHandler, TansAcquireRequest>> tasks, String msg) {
            for (Pair<TansHttpRequestHandler, TansAcquireRequest> p : tasks) {
                TansAcquireRequest request = p.getRight();

                FullHttpResponse response = p.getLeft().createAcquireResponseForError(request, msg);
                writeResponse(request.ctx, request.keepAlive, response);
            }
        }

        private void sendRedirect(List<Pair<TansHttpRequestHandler, TansAcquireRequest>> tasks, int otherLeaderId) {
            for (Pair<TansHttpRequestHandler, TansAcquireRequest> p : tasks) {
                TansAcquireRequest request = p.getRight();

                FullHttpResponse response = p.getLeft().createAcquireResponseForRedirect(request, otherLeaderId);
                writeResponse(request.ctx, request.keepAlive, response);
            }
        }
    }
}

