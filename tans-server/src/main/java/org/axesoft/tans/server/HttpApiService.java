package org.axesoft.tans.server;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * The server serving the client HTTP take a number request
 */
public class HttpApiService extends AbstractExecutionThreadService {
    private static final Logger logger = LoggerFactory.getLogger(HttpApiService.class);

    private static final String SERVICE_NAME = "Take-A-Number System";

    /**
     * The auth token is asked to be carried in header
     */
    private static final String TOKEN_HEADER_NAME = "X-tans-token";

    /**
     * Reusable "not found" message
     */
    private static final ByteBuf NOT_FOUND_BYTEBUF = Unpooled.copiedBuffer("Not Found\n", CharsetUtil.UTF_8);

    /**
     * The global configurations
     */
    private TansConfig config;

    /**
     * The actual service providing actual TANS function
     */
    private TansService tansService;

    /**
     * The server channel of the HTTP port
     */
    private Channel serverChannel;

    /**
     * The netty's thread pool handling network connecting action
     */
    private EventLoopGroup bossGroup;

    /**
     * The netty's thread pool handling each HTTP request
     */
    private EventLoopGroup workerGroup;
    /**
     * The request queue for each partition for batching the requests
     */
    private TansAcquireRequestQueue[] acquireRequestQueues;
    /**
     * The timer for flushing the queue in case of queue is not full but wait time is enough
     */
    private Timer queueFlushTimer;
    /**
     * Generator of srn for providing one if request does not curry it
     */
    private SeqGenerator requestSeqGenerator;

    /**
     * Metrics of this service
     */
    private TansMetrics metrics;

    /**
     * Map of request handlers for different API version. Although there is only one "v1" handler at present
     */
    private Map<String, TansHttpRequestHandler> handlerMap;

    /**
     * A const string for filling in the response's header "host"
     */
    private final String hostHeaderValue;

    /**
     * The object in the request queue for indicating an acquire request
     */
    private static class TansAcquireRequest {
        final KeyLong keyLong;
        final boolean keepAlive;
        final boolean ignoreLeader;
        final ChannelHandlerContext ctx;
        final long timestamp;

        public TansAcquireRequest(long rsn, String key, long v, boolean ignoreLeader, boolean keepAlive, ChannelHandlerContext ctx) {
            this.keyLong = new KeyLong(key, v, rsn);
            this.keepAlive = keepAlive;
            this.ignoreLeader = ignoreLeader;
            this.ctx = ctx;
            this.timestamp = System.currentTimeMillis();
        }
    }

    /**
     * Interface for different version of HTTP API
     */
    interface TansHttpRequestHandler {
        /**
         * call by server for handling the request. The dispatch method is based on the first path like "/v1/a/b/c", "/v2/x/y".
         * There is no any assumption except the first segment like "/v1", "/v2"
         */
        void process(String[] path, HttpRequest request, ChannelHandlerContext context);

        /**
         * Call by request queue for generating response when acquiring accomplished successfully
         * @param request the request not null
         * @param result the result not null
         * @return not null, a response which will be sent to the client
         */
        FullHttpResponse createAcquireResponseForSuccess(TansAcquireRequest request, LongRange result);

        /**
         * Call by request queue for generating response when the request should be redirect to another server
         * @param request the request
         * @param otherLeaderId which server should the request be redirected to
         * @return not null, a response which will be sent to the client
         */
        FullHttpResponse createAcquireResponseForRedirect(TansAcquireRequest request, int otherLeaderId);

        /**
         * Call by request queue for generating response when some error happen
         * @param request the request, not null
         * @param msg the error message
         * @return not null, a response which will be sent to the client
         */
        FullHttpResponse createAcquireResponseForError(TansAcquireRequest request, String msg);
    }


    public HttpApiService(TansConfig config, TansService tansService) {
        this.tansService = tansService;
        this.config = config;
        this.metrics = new TansMetrics(config.serverId(), this.config.jaxConfig().partitionNumber(), this.tansService::keyCountOf);
        this.requestSeqGenerator = new SeqGenerator(config.serverId());

        this.acquireRequestQueues = new TansAcquireRequestQueue[config.jaxConfig().partitionNumber()];
        for (int i = 0; i < acquireRequestQueues.length; i++) {
            this.acquireRequestQueues[i] = new TansAcquireRequestQueue(i, config.requestBatchSize());
        }

        this.handlerMap = ImmutableMap.of("v1", new TansHttpRequestHandlerV1());

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
        this.hostHeaderValue = config.address() + ":" + config.httpPort();
    }

    @Override
    protected void run() throws Exception {
        this.queueFlushTimer = new Timer("Queue Flush", true);
        this.queueFlushTimer.scheduleAtFixedRate(new TimerTask() {
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

    private static FullHttpResponse createMethodNotAllowedResponse(ChannelHandlerContext ctx, String url, String method) {
        return createResponse(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED,
                "Unsupported method \"", method, "\"",
                "for url ", url);
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
        if (!response.headers().contains(HttpHeaderNames.CONTENT_TYPE)) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        }
        response.headers().set(HttpHeaderNames.HOST, this.hostHeaderValue);
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-cache");
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

    private class TansHttpChannelHandler extends SimpleChannelInboundHandler {
        private HttpRequest request;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            metrics.incConnectCount();
        }

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
            if (request.uri().startsWith("/metrics")) {
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
        //The v1 service URL is http://<host>:<port>/v1/keys/<keyname>[?number=<number>][&ignoreleader=true|false]
        //PUT for acquiring an id range, GET for read the key value at present

        private static final String KEYS_ROOT_PATH = "keys";
        private static final String NUMBER_ARG_NAME = "number";
        private static final String SR_ARG_NAME = "srn";
        private static final String CLIENT_ID_PATH = "client-id";

        private final String NOT_FOUND_MESSAGE = "{\n\"message\":\"Not Found\"\n}\n";

        @Override
        public void process(String[] pathSegments, HttpRequest request, ChannelHandlerContext context) {
            String p1 = pathSegments.length > 0 ? pathSegments[0] : "";
            String p2 = pathSegments.length > 1 ? pathSegments[1] : "";
            FullHttpResponse response = null;
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
                    else {
                        response = createResponse(context, HttpResponseStatus.BAD_REQUEST, either.getLeft());
                    }
                }
                else if (request.method().name().equals("GET")) {
                    response = processKeyRead(p2, HttpUtil.isKeepAlive(request), context);
                }
                else {
                    response = createMethodNotAllowedResponse(context, request.uri(), request.method().name());
                }
            }
            else if (pathSegments.length == 1 && p1.equals(CLIENT_ID_PATH)) {
                if (request.method().name().equals("PUT")) {
                    processAcquireClientIdRequest(context, request);
                }
                else {
                    response = createMethodNotAllowedResponse(context, request.uri(), request.method().name());
                }
            }

            if (response == null) {
                response = this.createJsonResponse(context, HttpResponseStatus.NOT_FOUND, NOT_FOUND_MESSAGE);
            }
            writeResponse(context, false, response);
        }

        private Either<String, TansAcquireRequest> parseAcquireRequest(String p2, HttpRequest request, ChannelHandlerContext ctx) {
            if (Strings.isNullOrEmpty(p2)) {
                return Either.left("key name not provide");
            }

            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
            Either<String, Long> n = getLongParam(queryStringDecoder.parameters(), NUMBER_ARG_NAME, 1, 1);
            if (!n.isRight()) {
                return Either.castRight(n);
            }
            Either<String, Long> srn = getLongParam(queryStringDecoder.parameters(), SR_ARG_NAME, Long.MIN_VALUE, 0);
            if (!srn.isRight()) {
                return Either.castRight(srn);
            }

            long i = srn.getRight() == 0 ? requestSeqGenerator.next() : srn.getRight();
            return Either.right(new TansAcquireRequest(i, p2, n.getRight(), false, HttpUtil.isKeepAlive(request), ctx));
        }

        private void processAcquireClientIdRequest(ChannelHandlerContext context, HttpRequest request) {
            tansService.acquireClientId()
                    .handle((r, ex) -> {
                        FullHttpResponse resp;
                        if (r != null) {
                            if (r.isSuccess()) {
                                resp = createJsonResponse(context, HttpResponseStatus.OK,
                                        "{\n",
                                        "\"client-id\": ", r.value().toString(), "\n",
                                        "}\n");
                            }
                            else if (r.isRedirect()) {
                                resp = createJsonResponse(context, HttpResponseStatus.TEMPORARY_REDIRECT);
                                int httpPort = config.getPeerHttpPort(r.redirectServerId());
                                JaxosSettings.Peer peer = config.jaxConfig().getPeer(r.redirectServerId());
                                resp.headers().set(HttpHeaderNames.LOCATION,
                                        String.format("http://%s:%s/v1/%s", peer.address(), httpPort, CLIENT_ID_PATH));
                            }
                            else {
                                resp = createResponse(context, HttpResponseStatus.SERVICE_UNAVAILABLE);
                            }
                        }
                        else {
                            logger.error("When acquireClientId ", ex);
                            resp = createResponse(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, "INTERNAL ERROR");
                        }
                        writeResponse(context, HttpUtil.isKeepAlive(request), resp);
                        return null;
                    });
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
            response.headers().add(HttpHeaderNames.LAST_MODIFIED, new Date(result.timestamp()).toString());
            return response;
        }

        @Override
        public FullHttpResponse createAcquireResponseForRedirect(TansAcquireRequest request, int otherLeaderId) {
            int httpPort = config.getPeerHttpPort(otherLeaderId);
            JaxosSettings.Peer peer = config.jaxConfig().getPeer(otherLeaderId);

            StringBuffer sb = new StringBuffer();
            sb.append("http://").append(peer.address()).append(":").append(httpPort);
            sb.append("/v1/keys/").append(request.keyLong.key());
            sb.append("?").append(NUMBER_ARG_NAME).append("=").append(request.keyLong.value());
            if (!requestSeqGenerator.isTansGenerate(request.keyLong.stamp())) {
                sb.append("?").append(SR_ARG_NAME).append("=").append(String.format("0x%x", request.keyLong.stamp()));
            }

            FullHttpResponse response = this.createJsonResponse(request.ctx, HttpResponseStatus.TEMPORARY_REDIRECT);
            response.headers().set(HttpHeaderNames.LOCATION, sb.toString());
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


        private FullHttpResponse processKeyRead(String keyName, boolean keepAlive, ChannelHandlerContext context) {
            Optional<TansNumber> opt = tansService.localValueOf(keyName);
            if (opt.isPresent()) {
                TansNumber n = opt.get();
                return this.createJsonResponse(context, HttpResponseStatus.OK,
                        "{\n",
                        "\"key\": ", "\"", n.name(), "\",\n",
                        "\"value\": ", Long.toString(n.value()), ",\n",
                        "\"version\":  ", Long.toString(n.version()), ",\n",
                        "\"lastModified\": \"", new Date(n.timestamp()).toString(), "\"\n",
                        "}\r\n");
            }
            return null;
        }

        private FullHttpResponse createJsonResponse(ChannelHandlerContext ctx, HttpResponseStatus status, String... vx) {
            FullHttpResponse r = HttpApiService.createResponse(ctx, status, vx);
            r.headers().add(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
            return r;
        }
    }

    private static Either<String, Long> getLongParam(Map<String, List<String>> params, String paramName, long minValue, long deft) {
        long n = deft;
        List<String> nx = params.get(paramName);
        if (nx != null && nx.size() > 0) {
            String s0 = nx.get(0);
            int radix = 10;
            String s1 = s0;
            if (s0.startsWith("0x") || s0.startsWith("0X")) {
                s1 = s0.substring(2);
                radix = 16;
            }

            try {
                n = Long.parseLong(s1, radix);
                if (n < minValue) {
                    return Either.left("Zero or negative n '" + n + "'");
                }
            }
            catch (NumberFormatException e) {
                return Either.left("Invalid number '" + nx.get(0) + "'");
            }
        }
        return Either.right(n);
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

    public static class SeqGenerator {

        private long highBits;
        private AtomicInteger serialNum;
        private static final long MAGIC_MASK = 0xf000_0000_0000_0000L;

        public SeqGenerator(int serverId) {
            if (serverId > 255 || serverId <= 0) {
                throw new IllegalArgumentException("Seq generator does not support server id great than 255");
            }
            byte[] ip = getIpAddress();
            long ipMask = Ints.fromByteArray(ip);
            this.highBits = (ipMask << 32) | (((long) serverId) << 24) | MAGIC_MASK;
            this.serialNum = new AtomicInteger(0);
        }

        public long next() {
            return this.highBits | (this.serialNum.getAndIncrement() & 0xff_ffffL); //low 24 bits
        }

        public boolean isTansGenerate(long srn) {
            return (srn & MAGIC_MASK) == MAGIC_MASK;
        }

        private byte[] getIpAddress() {
            try {
                Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                while (networkInterfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = networkInterfaces.nextElement();
                    if (networkInterface.isLoopback()) {
                        continue;
                    }
                    Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress addr = addresses.nextElement();
                        if (addr instanceof Inet4Address) {
                            return addr.getAddress();
                        }
                    }
                }
            }
            catch (SocketException e) {
                throw new RuntimeException(e);
            }

            return new byte[]{127, 0, 0, 1};
        }

    }
}

