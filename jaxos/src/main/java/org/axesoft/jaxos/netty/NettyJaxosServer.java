package org.axesoft.jaxos.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.network.MessageCoder;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class NettyJaxosServer {
    private static Logger logger = LoggerFactory.getLogger(NettyJaxosServer.class);

    private JaxosSettings settings;
    private MessageCoder<PaxosMessage.DataGram> messageCoder;
    private Channel serverChannel;
    private EventWorkerPool workerPool;
    private JaxosChannelHandler jaxosChannelHandler;

    public NettyJaxosServer(JaxosSettings settings, EventWorkerPool workerPool) {
        this.settings = settings;
        this.messageCoder = new ProtoMessageCoder();
        this.workerPool = workerPool;

        this.jaxosChannelHandler = new JaxosChannelHandler();
    }

    public void startup() {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(16);
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT);

            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.config().setAllocator(UnpooledByteBufAllocator.DEFAULT);

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
    public class JaxosChannelHandler extends ChannelInboundHandlerAdapter {
        private final PaxosMessage.DataGram heartBeatResponse = messageCoder.encode(new Event.HeartBeatResponse(settings.serverId()));

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.info("Channel connected from {}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof PaxosMessage.DataGram) {
                Event e0 = messageCoder.decode((PaxosMessage.DataGram) msg);
                if (e0.code() == Event.Code.HEART_BEAT) {
                    //logger.info("Receive heart beat from server {}", e0.senderId());
                    ctx.writeAndFlush(heartBeatResponse);
                }
                else {
                    NettyJaxosServer.this.workerPool.submitEvent(e0, e1 -> {
                        PaxosMessage.DataGram response = messageCoder.encode(e1);
                        ctx.writeAndFlush(response);
                    });
                }
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER.retainedDuplicate());
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
