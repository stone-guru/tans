package org.axesoft.tans.server;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
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

    //The service URL is http://<host>:<port>/acquire?key=<keyname>[&n=<number>][&ignoreleader=true|false]
    private static final String ACQUIRE_PATH = "/acquire";
    private static final String KEY_ARG_NAME = "key";
    private static final String NUM_ARG_NAME = "n";
    private static final String IGNORE_LEADER_ARG_NAME = "ignoreleader";

    private static final String PARTITION_PATH = "/partition_number";
    private static final String METRICS_PATH = "/metrics";

    private static final String ARG_KEY_REQUIRED_MSG = "argument of '" + KEY_ARG_NAME + "' is required";

    private static final ByteBuf NOT_FOUND_BYTEBUF = Unpooled.copiedBuffer("Not Found", CharsetUtil.UTF_8);

    private TansService tansService;
    private TansConfig config;

    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private RequestQueue[] requestQueues;
    private Timer queueTimer;

    private TansMetrics metrics;

    public HttpApiService(TansConfig config, TansService tansService) {
        this.tansService = tansService;
        this.config = config;
        this.metrics = new TansMetrics(config.serverId(), this.config.jaxConfig().partitionNumber(), this.tansService::keyCountOf);

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
        //this.threadPool = new PartedThreadPool(this.config.jaxConfig().partitionNumber(), "API Thread");

        this.requestQueues = new RequestQueue[config.jaxConfig().partitionNumber()];
        for (int i = 0; i < requestQueues.length; i++) {
            this.requestQueues[i] = new RequestQueue(i, config.requestBatchSize());
        }
        this.queueTimer = new Timer("Queue Flush", true);
        this.queueTimer.scheduleAtFixedRate(new TimerTask() {
            private boolean interrupted = false;

            @Override
            public void run() {
                if (interrupted) {
                    return;
                }
                long current = System.currentTimeMillis();
                for (RequestQueue q : requestQueues) {
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
                            p.addLast(new HttpChannelHandler());
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
        if(vx == null || vx.length == 0){
            buf = Unpooled.copiedBuffer("\r\n", CharsetUtil.UTF_8);
        } else {
            buf = ctx.alloc().directBuffer();
            for(String s : vx){
                buf.writeCharSequence(s, CharsetUtil.UTF_8);
            }
            //buf.writeCharSequence("\r\n", CharsetUtil.UTF_8);
        }
        return new DefaultFullHttpResponse(HTTP_1_1, status, buf);
    }

    private FullHttpResponse createRedirectResponse(int serverId, String key, long n) {
        int httpPort = config.getPeerHttpPort(serverId);
        JaxosSettings.Peer peer = config.jaxConfig().getPeer(serverId);
        String url = String.format("http://%s:%s%s?%s=%s&%s=%d",
                peer.address(), httpPort, ACQUIRE_PATH,
                KEY_ARG_NAME, key,
                NUM_ARG_NAME, n);

        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, HttpResponseStatus.TEMPORARY_REDIRECT);
        response.headers().set(HttpHeaderNames.LOCATION, url);
        return response;
    }


    private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, FullHttpResponse response) {
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
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

    public class HttpChannelHandler extends SimpleChannelInboundHandler {
        private HttpRequest request;
        private boolean keepAlive;

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                this.request = (HttpRequest) msg;
                this.keepAlive = HttpUtil.isKeepAlive(request);
                if (HttpUtil.is100ContinueExpected(request)) {
                    send100Continue(ctx);
                }
            }

            if (msg instanceof LastHttpContent) {

                final String rawUrl = getRawUri(request.uri());
                if (ACQUIRE_PATH.equals(rawUrl)) {
                    Either<String, HttpAcquireRequest> either = parseAcquireRequest(request, this.keepAlive, ctx);
                    if (!either.isRight()) {
                        FullHttpResponse r = HttpApiService.createResponse(ctx, HttpResponseStatus.BAD_REQUEST,either.getLeft());
                        writeResponse(ctx, keepAlive, r);
                        return;
                    }

                    HttpAcquireRequest httpAcquireRequest = either.getRight();
                    if (logger.isTraceEnabled()) {
                        logger.trace("Got request {}", httpAcquireRequest.keyLong);
                    }
                    int i = tansService.squadIdOf(httpAcquireRequest.keyLong.key());
                    requestQueues[i].addRequest(httpAcquireRequest);
                }
                else if (PARTITION_PATH.equals(rawUrl)) {
                    String s = Integer.toString(config.jaxConfig().partitionNumber());
                    FullHttpResponse r = HttpApiService.createResponse(ctx, HttpResponseStatus.OK, s);
                    writeResponse(ctx, keepAlive, r);
                }
                else if (METRICS_PATH.equals(rawUrl)) {
                    String s1 = tansService.formatMetrics();
                    String s2 = metrics.format();
                    FullHttpResponse r = HttpApiService.createResponse(ctx, HttpResponseStatus.OK, s1, s2);
                    writeResponse(ctx, false, r);
                }
                else {
                    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                            NOT_FOUND_BYTEBUF.retainedDuplicate());
                    writeResponse(ctx, keepAlive, response);
                }
            }
        }

        private String getRawUri(String s) {
            int i = s.indexOf("?");
            if (i < 0) {
                return s;
            }
            else {
                return s.substring(0, i);
            }
        }

        private Either<String, HttpAcquireRequest> parseAcquireRequest(HttpRequest request, boolean keepAlive, ChannelHandlerContext ctx) {
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

            return Either.right(new HttpAcquireRequest(key, n, ignoreLeader, keepAlive, ctx));
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

    private static class HttpAcquireRequest {
        final KeyLong keyLong;
        final boolean keepAlive;
        final boolean ignoreLeader;
        final ChannelHandlerContext ctx;
        final long timestamp;

        public HttpAcquireRequest(String key, long v, boolean ignoreLeader, boolean keepAlive, ChannelHandlerContext ctx) {
            this.keyLong = new KeyLong(key, v);
            this.keepAlive = keepAlive;
            this.ignoreLeader = ignoreLeader;
            this.ctx = ctx;
            this.timestamp = System.currentTimeMillis();
        }
    }

    private class RequestQueue {
        private final int squadId;
        private final int waitingSize;

        private long t0;
        private List<HttpAcquireRequest> waitingTasks;
        private RateLimiter logRateLimiter;

        public RequestQueue(int squadId, int waitingSize) {
            this.squadId = squadId;
            this.waitingSize = waitingSize;
            this.t0 = 0;
            this.waitingTasks = new ArrayList<>(waitingSize);
            this.logRateLimiter = RateLimiter.create(1.0 / 2.0);
        }

        private synchronized void addRequest(HttpAcquireRequest request) {
            if (logger.isTraceEnabled()) {
                logger.trace("S{} RequestQueue accept request {}", squadId, request);
            }
            HttpApiService.this.metrics.incRequestCount();

            this.waitingTasks.add(request);
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
            final List<HttpAcquireRequest> todo = ImmutableList.copyOf(this.waitingTasks);
            this.waitingTasks.clear();
            this.t0 = 0;

            //threadPool.submit(this.squadId, () -> process(todo));
            workerGroup.submit(() -> process(todo));
        }

        private void process(List<HttpAcquireRequest> tasks) {
            boolean ignoreLeader = false;
            List<KeyLong> kvx = new ArrayList<>(tasks.size());
            for (HttpAcquireRequest req : tasks) {
                kvx.add(req.keyLong);
                if (req.ignoreLeader) {
                    ignoreLeader = true;
                }
            }

            final long startMillis = System.currentTimeMillis();
            tansService.acquire(squadId, kvx, ignoreLeader)
                    .thenAcceptAsync(r -> {
                        switch (r.code()) {
                            case SUCCESS:
                                onProcessSuccess(tasks, r.value(), startMillis);
                                break;
                            case REDIRECT:
                                HttpApiService.this.metrics.incRedirectCounter();
                                sendRedirect(tasks, r.redirectServerId());
                                break;
                            case ERROR:
                                onProcessFailure(tasks, r.errorMessage());
                        }
                    }, workerGroup);
        }

        public void onProcessSuccess(List<HttpAcquireRequest> tasks, List<LongRange> result, long t0) {
            for (int i = 0; i < tasks.size(); i++) {
                HttpAcquireRequest task = tasks.get(i);
                FullHttpResponse response;
                LongRange r = result.get(i);
                response = createResponse(task.ctx, HttpResponseStatus.OK,
                        task.keyLong.key(), ",", Long.toString(r.low()), ",", Long.toString(r.high()), "\r\n");
                if (logger.isTraceEnabled()) {
                    logger.trace("S{} write response {},{},{},{}", squadId,
                            task.keyLong.key(), task.keyLong.value(), r.low(), r.high());
                }
                writeResponse(task.ctx, task.keepAlive, response);

                HttpApiService.this.metrics.recordRequestElapsed(t0 - task.timestamp);
            }
        }

        public void onProcessFailure(List<HttpAcquireRequest> tasks, String msg) {
            for (int i = 0; i < tasks.size(); i++) {
                HttpAcquireRequest task = tasks.get(i);
                FullHttpResponse response = createResponseForError(msg, task.ctx);
                writeResponse(task.ctx, task.keepAlive, response);
            }
        }

        private void sendRedirect(List<HttpAcquireRequest> tasks, int otherLeaderId) {
            for (int i = 0; i < tasks.size(); i++) {
                HttpAcquireRequest task = tasks.get(i);
                FullHttpResponse response = createRedirectResponse(otherLeaderId, task.keyLong.key(), task.keyLong.value());
                writeResponse(task.ctx, task.keepAlive, response);
            }
        }

        private FullHttpResponse createResponseForError(String msg, ChannelHandlerContext ctx) {
            return createResponse(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, msg, "\r\n");

//            if (t instanceof ProposalConflictException) {
//                return createResponse(HttpResponseStatus.CONFLICT, "CONFLICT");
//            }
//            else if (t instanceof TerminatedException) {
//                if (logRateLimiter.tryAcquire()) {
//                    logger.info("Jaxos service stopped, ignore response");
//                }
//                return null;
//            }
//            else if (t instanceof NoQuorumException) {
//                return createResponse(HttpResponseStatus.SERVICE_UNAVAILABLE, "SERVICE UNAVAILABLE");
//            }
//            else if (t instanceof TimeoutException) {
//                logger.warn("Acquire timeout", t);
//                return createResponse(HttpResponseStatus.SERVICE_UNAVAILABLE, "SERVICE UNAVAILABLE");
//            }
//            else if (t instanceof RuntimeException) {
//                if (t.getCause() instanceof TimeoutException) {
//                    logger.warn("Acquire timeout", t);
//                    return createResponse(HttpResponseStatus.SERVICE_UNAVAILABLE, "SERVICE UNAVAILABLE");
//                }
//                else {
//                    logger.error("Process ", t);
//                    return createResponse(INTERNAL_SERVER_ERROR, "INTERNAL ERROR");
//                }
//            }
//            else {
//                logger.error("Process ", t);
//                return createResponse(INTERNAL_SERVER_ERROR, "INTERNAL ERROR");
//            }
        }


    }
}

