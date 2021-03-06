package org.axesoft.jaxos.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.base.GroupedRateLimiter;
import org.axesoft.jaxos.base.NumberedThreadFactory;
import org.axesoft.jaxos.network.MessageCoder;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class NettyJaxosServer {
    private final static AttributeKey<Integer> ATTR_PEER_ID = AttributeKey.newInstance("PEER_ID");

    private static Logger logger = LoggerFactory.getLogger(NettyJaxosServer.class);

    private JaxosSettings settings;
    private MessageCoder<PaxosMessage.DataGram> messageCoder;
    private Channel serverChannel;
    private EventWorkerPool workerPool;
    private JaxosChannelHandler jaxosChannelHandler;
    private GroupedRateLimiter peerRateLimiter;

    public NettyJaxosServer(JaxosSettings settings, EventWorkerPool workerPool) {
        this.settings = settings;
        this.messageCoder = new ProtoMessageCoder();
        this.workerPool = workerPool;

        this.jaxosChannelHandler = new JaxosChannelHandler();
        this.peerRateLimiter = new GroupedRateLimiter(1.0 / 5.0);
    }

    public void start() {
        EventLoopGroup boss = new NioEventLoopGroup(1,
                new NumberedThreadFactory("NettyJaxosBoss"));
        EventLoopGroup worker = new NioEventLoopGroup(this.settings.nettyJaxosWorkerThreadNumber(),
                new NumberedThreadFactory("NettyJaxosWorker"));
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.config().setAllocator(PooledByteBufAllocator.DEFAULT);

                    socketChannel.pipeline()
                            .addLast(new IdleStateHandler(20, 20, 20 * 10, TimeUnit.SECONDS))
                            .addLast(new ProtobufVarint32FrameDecoder())
                            .addLast(new ProtobufDecoder(PaxosMessage.DataGram.getDefaultInstance()))
                            .addLast(new ProtobufVarint32LengthFieldPrepender())
                            .addLast(new ProtobufEncoder())
                            .addLast(NettyJaxosServer.this.jaxosChannelHandler);
                }
            });
            ChannelFuture channelFuture = serverBootstrap.bind(settings.self().port()).sync();
            this.serverChannel = channelFuture.channel();
            channelFuture.channel().closeFuture().sync();
        }
        catch (Exception e) {
            logger.error("Netty server error", e);
        }
        finally {
            try {
                worker.shutdownGracefully().sync();
                boss.shutdownGracefully().sync();
            }
            catch (Exception e) {
                logger.info("error when shutdown netty jaxos server ", e);
            }
        }
    }

    public void shutdown() {
        this.serverChannel.close();
    }


    @ChannelHandler.Sharable
    public class JaxosChannelHandler extends SimpleChannelInboundHandler<PaxosMessage.DataGram> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.info("Socket connected from {}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, PaxosMessage.DataGram msg) throws Exception {
            Event e0 = messageCoder.decode(msg);

            Integer peerId = ctx.channel().attr(ATTR_PEER_ID).get();
            //this is the first
            if (peerId == null) {
                ctx.channel().attr(ATTR_PEER_ID).set(e0.senderId());
            }
            else if (e0.senderId() != peerId) {
                if(peerRateLimiter.tryAcquireFor(peerId)){
                    logger.warn("Phony server id({}) while being {} at the beginning!", e0.senderId(), peerId);
                }
                return;
            }

            NettyJaxosServer.this.workerPool.submitEvent(e0, e1 -> {
                PaxosMessage.DataGram response = messageCoder.encode(e1);
                ctx.writeAndFlush(response);
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            Integer peerId = ctx.channel().attr(ATTR_PEER_ID).get();
            if(peerId != null){
                NettyJaxosServer.this.workerPool.submitEventToSelf(new Event.PeerLeft(settings.serverId(), peerId));
            }
            super.channelInactive(ctx);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            //ctx.writeAndFlush(Unpooled.EMPTY_BUFFER.retainedDuplicate());
            super.channelReadComplete(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("error when handle request", cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object obj) throws Exception {
            super.userEventTriggered(ctx, obj);
        }
    }

}
