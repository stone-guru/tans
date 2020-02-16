package org.axesoft.tans.server;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
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
import io.netty.util.ReferenceCountUtil;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.TerminatedException;
import org.axesoft.jaxos.algo.NoQuorumException;
import org.axesoft.jaxos.algo.ProposalConflictException;
import org.axesoft.jaxos.algo.RedirectException;
import org.axesoft.jaxos.base.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeoutException;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * An HTTP server that sends back the content of the received HTTP request
 * in a pretty plaintext form.
 */
public final class HttpApiService extends AbstractExecutionThreadService {
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
    private PartedThreadPool threadPool;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private RequestQueue[] requestQueues;
    private Timer queueTimer;

    private TansMetrics metrics;

    public HttpApiService(TansConfig config, TansService tansService, PartedThreadPool requestThreadPool) {
        this.tansService = tansService;
        this.config = config;
        this.threadPool = requestThreadPool;
        this.metrics = new TansMetrics(config.serverId(), this.config.jaxConfig().partitionNumber(), this.tansService::keyCountOf, this.threadPool::queueSizeOf);

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

        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup(3);
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

    private FullHttpResponse createResponse(HttpResponseStatus status, String v) {
        return new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(v + "\r\n", CharsetUtil.UTF_8));
    }

    private FullHttpResponse createRedirectResponse(int serverId, String key, long n) {
        int httpPort = config.getPeerHttpPort(serverId);
        JaxosSettings.Peer peer = config.jaxConfig().getPeer(serverId);
        String url = String.format("http://%s:%s%s?%s=%s&%s=%d",
                peer.address(), httpPort, ACQUIRE_PATH,
                KEY_ARG_NAME, key,
                NUM_ARG_NAME, n);

        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, TEMPORARY_REDIRECT);
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

    public class HttpChannelHandler extends ChannelInboundHandlerAdapter {
        private HttpRequest request;
        private boolean keepAlive;

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                this.request = (HttpRequest) msg;
                this.keepAlive = HttpUtil.isKeepAlive(request);
                if (HttpUtil.is100ContinueExpected(request)) {
                    send100Continue(ctx);
                }
            }

            if (msg instanceof LastHttpContent) {
                try {
                    final String rawUrl = getRawUri(request.uri());
                    if (ACQUIRE_PATH.equals(rawUrl)) {
                        Either<String, HttpAcquireRequest> either = parseAcquireRequest(request, this.keepAlive, ctx);
                        if (!either.isRight()) {
                            writeResponse(ctx, keepAlive,
                                    new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST,
                                            Unpooled.copiedBuffer(either.getLeft(), CharsetUtil.UTF_8)));
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
                        writeResponse(ctx, keepAlive,
                                new DefaultFullHttpResponse(HTTP_1_1, OK,
                                        Unpooled.copiedBuffer(s + "\r\n", CharsetUtil.UTF_8)));
                    }
                    else if (METRICS_PATH.equals(rawUrl)) {
                        String s1 = tansService.formatMetrics();
                        String s2 = metrics.format();
                        writeResponse(ctx, false,
                                new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.copiedBuffer(s1 + s2, CharsetUtil.UTF_8)));
                    }
                    else {
                        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                                NOT_FOUND_BYTEBUF.retainedDuplicate());
                        writeResponse(ctx, keepAlive, response);
                    }
                }
                finally {
                    ReferenceCountUtil.release(request);
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
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER);
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
        private List<HttpAcquireRequest> tasks;
        private RateLimiter logRateLimiter;

        public RequestQueue(int squadId, int waitingSize) {
            this.squadId = squadId;
            this.waitingSize = waitingSize;
            this.t0 = 0;
            this.tasks = new ArrayList<>(waitingSize);
            this.logRateLimiter = RateLimiter.create(1.0 / 2.0);
        }

        private synchronized void addRequest(HttpAcquireRequest request) {
            if (logger.isTraceEnabled()) {
                logger.trace("S{} RequestQueue accept request {}", squadId, request);
            }
            HttpApiService.this.metrics.incRequestCount();

            this.tasks.add(request);
            if (this.tasks.size() == 1) {
                this.t0 = System.currentTimeMillis();
            }

            if (tasks.size() >= waitingSize) {
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
            final List<HttpAcquireRequest> todo = ImmutableList.copyOf(this.tasks);
            this.tasks.clear();
            this.t0 = 0;

            threadPool.submit(this.squadId, () -> process(todo));
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

            List<LongRange> rx = null;
            FullHttpResponse errorResponse = null;
            int otherLeaderId = -1;
            try {
                rx = tansService.acquire(squadId, kvx, ignoreLeader);
            }
            catch (RedirectException e) {
                otherLeaderId = e.getServerId();
                HttpApiService.this.metrics.incRedirectCounter();
            }
            catch (ProposalConflictException e) {
                errorResponse = createResponse(HttpResponseStatus.CONFLICT, "CONFLICT");
            }
            catch (TerminatedException e) {
                if (logRateLimiter.tryAcquire()) {
                    logger.info("Jaxos service stopped, ignore response");
                }
                return;
            }
            catch (NoQuorumException e) {
                errorResponse = createResponse(HttpResponseStatus.SERVICE_UNAVAILABLE, "SERVICE UNAVAILABLE");
            }
            catch (RuntimeException e) {
                if(e.getCause() instanceof TimeoutException){
                    logger.warn("Acquire timeout", e);
                    errorResponse = createResponse(HttpResponseStatus.SERVICE_UNAVAILABLE, "SERVICE UNAVAILABLE");
                } else {
                    logger.error("Process ", e);
                    errorResponse = createResponse(INTERNAL_SERVER_ERROR, "INTERNAL ERROR");
                }
            }catch(Throwable t){
                logger.error("Process ", t);
                errorResponse = createResponse(INTERNAL_SERVER_ERROR, "INTERNAL ERROR");
            }


            try {
                long current = System.currentTimeMillis();
                for (int i = 0; i < kvx.size(); i++) {
                    HttpAcquireRequest task = tasks.get(i);

                    FullHttpResponse response;
                    if (errorResponse != null) {
                        response = errorResponse.retainedDuplicate();
                    }
                    else if (otherLeaderId >= 0) {
                        response = createRedirectResponse(otherLeaderId, task.keyLong.key(), task.keyLong.value());
                    }
                    else {
                        LongRange r = rx.get(i);
                        String content = task.keyLong.key() + "," + r.low() + "," + r.high();
                        response = createResponse(OK, content);
                        if (logger.isTraceEnabled()) {
                            logger.trace("S{} write response {},{},{},{}", squadId,
                                    task.keyLong.key(), task.keyLong.value(), r.low(), r.high());
                        }
                    }

                    writeResponse(task.ctx, task.keepAlive, response);

                    HttpApiService.this.metrics.recordRequestElapsed(current - task.timestamp);
                }
            }
            finally {
                if (errorResponse != null) {
                    ReferenceCountUtil.release(errorResponse);
                }
            }
        }
    }

}
