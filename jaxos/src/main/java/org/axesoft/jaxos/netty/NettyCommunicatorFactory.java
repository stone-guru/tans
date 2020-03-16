package org.axesoft.jaxos.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.EventWorkerPool;
import org.axesoft.jaxos.base.GroupedRateLimiter;
import org.axesoft.jaxos.base.NumberedThreadFactory;
import org.axesoft.jaxos.network.CommunicatorFactory;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.Communicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class NettyCommunicatorFactory implements CommunicatorFactory {
    private final static AttributeKey<JaxosSettings.Peer> ATTR_PEER = AttributeKey.newInstance("PEER");

    private final static AttributeKey<Boolean> JOIN_FAIL = AttributeKey.newInstance("JOIN_FAIL");
    private final static AttributeKey<Object> JOINED_BOTH = AttributeKey.newInstance("JOINED_BOTH");

    private static Logger logger = LoggerFactory.getLogger(NettyCommunicatorFactory.class);

    private JaxosSettings config;
    private ProtoMessageCoder coder;
    private EventWorkerPool eventWorkerPool;
    private Function<Integer, Event> connectRequestProvider;

    public NettyCommunicatorFactory(JaxosSettings config, EventWorkerPool eventWorkerPool, Function<Integer, Event> connectRequestProvider) {
        this.config = config;
        this.coder = new ProtoMessageCoder();
        this.eventWorkerPool = eventWorkerPool;
        this.connectRequestProvider = connectRequestProvider;
    }

    @Override
    public Communicator createCommunicator() {
        EventLoopGroup worker = new NioEventLoopGroup(2, new NumberedThreadFactory("JaxosSenderThread"));
        ChannelGroupCommunicator c = new ChannelGroupCommunicator(worker);
        c.start();
        return c;
    }

    private class ChannelGroupCommunicator implements Communicator {
        private ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        private List<Channel> newerChannels = new CopyOnWriteArrayList<>();

        private Bootstrap bootstrap;
        private EventLoopGroup worker;
        private volatile boolean closed;

        private Map<Integer, ChannelId> channelIdMap = new ConcurrentHashMap<>();
        private GroupedRateLimiter rateLimiter = new GroupedRateLimiter(1.0 / 15.0);

        public ChannelGroupCommunicator(EventLoopGroup worker) {
            Bootstrap bootstrap;
            try {
                bootstrap = new Bootstrap()
                        .group(worker)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel socketChannel) throws Exception {
                                ChannelPipeline pipeline = socketChannel.pipeline();
                                pipeline.addLast(new LoggingHandler(LogLevel.DEBUG))
                                        .addLast(new ProtobufVarint32FrameDecoder())
                                        .addLast(new ProtobufDecoder(PaxosMessage.DataGram.getDefaultInstance()))
                                        .addLast(new ProtobufVarint32LengthFieldPrepender())
                                        .addLast(new ProtobufEncoder())
                                        //.addLast(new JaxosOutboundHandler())
                                        .addLast(new JaxosClientHandler(ChannelGroupCommunicator.this));
                            }
                        });

            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.closed = false;
            this.worker = worker;
            this.bootstrap = bootstrap;
        }

        public void start() {
            for (JaxosSettings.Peer peer : config.peerMap().values()) {
                if (peer.id() != config.serverId()) {
                    connect(peer);
                }
            }
        }

        private void connect(JaxosSettings.Peer peer) {
            if (this.closed) {
                return;
            }

            ChannelFuture future;
            try {
                future = bootstrap.connect(new InetSocketAddress(peer.address(), peer.port()));
            }
            catch (Exception e) {
                if (this.closed) {
                    logger.info("Abandon connecting due to communicator closed: {}", e.getMessage());

                }
                else {
                    logger.warn("call connect fail", e);
                    if (!worker.isShuttingDown()) {
                        worker.schedule(() -> connect(peer), 1, TimeUnit.SECONDS);
                    }
                }
                return;
            }

            try {
                future.addListener(f -> {
                    if (!f.isSuccess()) {
                        if (f.cause() instanceof RejectedExecutionException) {
                            logger.info("Abandon connecting due to bootstrap closed: {}", f.cause().getMessage());
                            return;
                        }
                        if (rateLimiter.tryAcquireFor(peer.id())) {
                            logger.warn("Unable to connect to {} ", peer);
                        }
                        if (!worker.isShuttingDown()) {
                            worker.schedule(() -> connect(peer), 2, TimeUnit.SECONDS);
                        }
                    }
                    else {
                        logger.info("Socket created to {}", peer);
                        initChannel(((ChannelFuture) f).channel(), peer);
                    }
                });
            }
            catch (RejectedExecutionException e) {
                if (!this.closed) {
                    logger.error("Unable to attach listener for connection when connect to " + peer, e);
                }
            }
        }

        private void initChannel(Channel c, JaxosSettings.Peer peer) {
            c.attr(ATTR_PEER).set(peer);
            PaxosMessage.DataGram d = coder.encode(connectRequestProvider.apply(peer.id()));
            c.writeAndFlush(d);
            newerChannels.add(c);
            //FIXME handle join request timeout
        }

        private void onConnectResponseEvent(Event.JoinResponse event, Channel channel) {
            if (event.success()) {
                channels.add(channel);
                channelIdMap.put(channel.attr(ATTR_PEER).get().id(), channel.id());
                logger.info("Success join peer {}", channel.attr(ATTR_PEER).get());
            }
            else {
                logger.info("Failed to join  peer {} due to {}", channel.attr(ATTR_PEER).get(), event.message());
                channel.attr(JOIN_FAIL).set(true);
                channel.close();
            }
        }

        @Override
        public boolean available() {
            return !closed & channels.size() + 1 >= config.peerCount() / 2;
        }

        @Override
        public void broadcast(Event event) {
            if (logger.isTraceEnabled()) {
                logger.trace("Broadcast {} ", event);
            }
            sendByChannels(event);
            eventWorkerPool.submitEventToSelf(event);
        }

        @Override
        public void broadcastOthers(Event event) {
            if (logger.isTraceEnabled()) {
                logger.trace("Broadcast to others {} ", event);
            }
            sendByChannels(event);
        }

        private void sendByChannels(Event event) {
            PaxosMessage.DataGram dataGram = coder.encode(event);
            channels.writeAndFlush(dataGram);
        }

        @Override
        public void selfFirstBroadcast(Event event) {
            eventWorkerPool.directCallSelf(event);

            PaxosMessage.DataGram dataGram = coder.encode(event);
            ChannelId id = channelIdMap.get(config.serverId());
            channels.writeAndFlush(dataGram, c -> !c.id().equals(id));
        }

        @Override
        public void send(Event event, int serverId) {
            if (serverId == config.serverId()) {
                eventWorkerPool.submitEventToSelf(event);
            }
            else {
                PaxosMessage.DataGram dataGram = coder.encode(event);
                ChannelId id = channelIdMap.get(serverId);
                channels.writeAndFlush(dataGram, c -> c.id().equals(id));
            }
        }

        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            Channel c = ctx.channel();
            channels.remove(c);

            JaxosSettings.Peer peer = c.attr(ATTR_PEER).get();
            logger.warn("Disconnected from a server {}", peer);

            if (peer != null) {
                channelIdMap.remove(peer.id());

                //The peer has join to me by this channel
                if(c.hasAttr(JOINED_BOTH)){
                    eventWorkerPool.submitEventToSelf(new Event.PeerLeft(config.serverId(), peer.id()));
                }

                //Channel is closed proactively of login failed, re-try it after 10 seconds
                if (c.attr(JOIN_FAIL).get() != null) {
                    worker.schedule(() -> connect(peer), 10, TimeUnit.SECONDS);
                }
                else {
                    connect(peer);
                }
            }
            else {
                logger.error("No bind peer on channel");
            }
        }

        @Override
        public void close() {
            try {
                closed = true;
                channels.close().sync();
                worker.shutdownGracefully().sync();
            }
            catch (InterruptedException e) {
                logger.info("Interrupted at communicator.close()");
            }
            catch (Exception e) {
                logger.error("error when do communicator.close()", e);
            }
        }
    }

    private class JaxosClientHandler extends SimpleChannelInboundHandler {
        private ChannelGroupCommunicator communicator;

        public JaxosClientHandler(ChannelGroupCommunicator communicator) {
            this.communicator = communicator;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            communicator.channelInactive(ctx);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof PaxosMessage.DataGram) {
                PaxosMessage.DataGram dataGram = (PaxosMessage.DataGram) msg;
                //this is an empty dataGram
                //TODO ingest why
                if (dataGram.getCode() == PaxosMessage.Code.NONE) {
                    return;
                }

                Event event = coder.decode(dataGram);
                if (event != null) {
                    if (event.code() == Event.Code.JOIN_RESPONSE) {
                        communicator.onConnectResponseEvent((Event.JoinResponse) event, ctx.channel());
                    }
                    //This is special part for haskell acceptor will send join request at this channel
                    else if (event.code() == Event.Code.JOIN_REQUEST) {
                        eventWorkerPool.submitEvent(event, e1 -> {
                            Event.JoinResponse resp = (Event.JoinResponse)e1;
                            PaxosMessage.DataGram response = coder.encode(resp);
                            ChannelFuture future = ctx.writeAndFlush(response);

                            if(resp.success()){
                               ctx.channel().attr(JOINED_BOTH).set(new Object());
                            } else {
                                future.addListener(ChannelFutureListener.CLOSE);
                            }
                        });
                    }
                    else {
                        eventWorkerPool.submitEventToSelf(event);
                    }
                }
            }
            else {
                String s = Objects.toString(msg);
                logger.error("Unknown received object {}", s.substring(0, Integer.min(100, s.length())));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if ("Connection reset by peer".equals(cause.getMessage())) {
                logger.warn("Connection to {} reseted by peer", ctx.channel().attr(ATTR_PEER).get());
            }
            else {
                logger.error("error ", cause);
            }
            ctx.close();
        }
    }
}

