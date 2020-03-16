package org.axesoft.tans.server;

import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.base.NumberedThreadFactory;

import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class EventSender {
    private static final Logger logger = LoggerFactory.getLogger(EventSender.class);

    private final int serverId;
    private final Timer proposeTimer;
    private final PrometheusMeterRegistry registry;

    private JaxosSettings settings;
    private Object joinSignal = new Object();
    private volatile boolean started = false;
    private AtomicInteger receivedCount = new AtomicInteger(0);
    EventLoopGroup worker = new NioEventLoopGroup(2, new NumberedThreadFactory("JaxosSenderThread"));
    private Bootstrap bootstrap;
    private ProtoMessageCoder coder;
    private String server;
    private volatile boolean remoteJoined = false;
    private volatile boolean iJoined = false;

    public EventSender(String server) {
        this.serverId = 1;
        this.server = server;
        this.settings = JaxosSettings.builder()
                .setServerId(this.serverId)
                .setAppMessageVersion("0.1.5")
                .addPeer(new JaxosSettings.Peer(1, "localhost", 9091))
                .addPeer(new JaxosSettings.Peer(9, "localhost", 9099))
                .setPartitionNumber(6)
                .build();
        this.coder = new ProtoMessageCoder();

        try {
            this.bootstrap = new Bootstrap()
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
                                    .addLast(new JaxosClientHandler());
                        }
                    });

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.registry.config().commonTags("server", Integer.toString(this.serverId));
        this.proposeTimer = Timer.builder("propose.duration")
                .description("The time for each propose")
                .publishPercentiles(0.5, 0.85, 0.9, 0.95, 0.98, 0.99)
                .sla(Duration.ofMillis(1), Duration.ofMillis(3), Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(100), Duration.ofMillis(1000))
                .distributionStatisticExpiry(Duration.ofMinutes(1))
                .register(registry);
    }

    private void close(){
        try {
            this.worker.shutdownGracefully().sync();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean isBothJoined(){
        return iJoined && remoteJoined;
    }

    private void consumeEvent(Event event) {
        Event.ChosenInfo chosenInfo = null;
        if(event.code() == Event.Code.PREPARE_RESPONSE) {
            Event.PrepareResponse resp = (Event.PrepareResponse) event;
            chosenInfo = resp.chosenInfo();
        } else if (event.code() == Event.Code.ACCEPT_RESPONSE) {
            Event.AcceptResponse resp = (Event.AcceptResponse) event;
            chosenInfo = resp.chosenInfo();
        }
        if(chosenInfo != null) {
            long rtt = System.currentTimeMillis() - chosenInfo.elapsedMillis();
            this.proposeTimer.record(Math.max(1, rtt), TimeUnit.MILLISECONDS);

            if(receivedCount.incrementAndGet() % 1997 == 0) {
                logger.info("{} instance id = {}, RTT= {}", event.code(), chosenInfo.instanceId() + 1, rtt);
            }
        }
    }

    private Event createConnectEvent(int serverId) {
        return new Event.JoinRequest(1, "ABCDEF", "localhost",
                3, "2.1.1", "3.2.1", "localhost");
    }


    private static Event.PrepareRequest createPrepareEvent(int serverId, long instanceId) {
        Event.ChosenInfo info = new Event.ChosenInfo(instanceId - 1, 234, System.currentTimeMillis());
        return new Event.PrepareRequest(
                serverId, (int)instanceId%4,
                instanceId, 0,
                234, info);
    }

    private static Event.AcceptRequest createAcceptEvent(int serverId, long instanceId, int squadId, Event.BallotValue value) {
        Event.ChosenInfo info = new Event.ChosenInfo(instanceId - 1, 234, System.currentTimeMillis());
        return Event.AcceptRequest.newBuilder(serverId,
                squadId, instanceId, 0)
                .setBallot(234)
                .setValue(value)
                .setChosenInfo(info)
                .build();
    }

    private Thread writerThread= null;
    private void start(List<Event> events, int rate, int times) throws Exception {
        ChannelFuture future = this.bootstrap.connect(this.server, 9099);
        future.addListener(f -> {
            if(f.isSuccess()){
                final Channel channel = ((ChannelFuture)f).channel();
                writerThread = new Thread(() -> {
                    try {
                        run(channel, events, rate, times);
                    }catch(Exception e){
                        logger.error("Error", e);
                    }
                });
                writerThread.start();
            } else {
                logger.error("Unable to connect to {}:{}", this.server, 9099);
                System.exit(1);
            }
        });
    }

    private void run(Channel channel, List<Event> events, int rate, int times) throws Exception {
        System.out.println("Wait joining each other");

        while(!isBothJoined()){
            Thread.sleep(100);
        }

        System.out.println();
        System.out.println("---- start test round ----");
        RateLimiter rateLimiter = RateLimiter.create(rate);
        Event event = null;

        for (int i = 1; i <= times; i++) {
            rateLimiter.acquire(1);
            event = events.get(i % events.size());

            Event.ChosenInfo chosenInfo = null;
            if(event instanceof Event.PrepareRequest){
                chosenInfo = ((Event.PrepareRequest)event).chosenInfo();
            } else if (event instanceof Event.AcceptRequest) {
                chosenInfo = ((Event.AcceptRequest)event).chosenInfo();
            }

            if(chosenInfo != null){
                chosenInfo.setElapsedMillis(System.currentTimeMillis());
            }

            PaxosMessage.DataGram d = coder.encode(event);
            channel.writeAndFlush(d);
            //System.out.println("writed event " + i);
        }
    }

    private static List<Event> generateEvents(int n){
        List<Event> events = new ArrayList<>();

        Event.PrepareRequest prepareReq = null;
        Event event = null;
        Event.BallotValue v = new Event.BallotValue(10889, Event.ValueType.APPLICATION, ByteString.copyFrom(new byte[800]));
        for (int i = 1; i <= n; i++) {
            if(i % 2 == 1) {
                event = prepareReq = createPrepareEvent(1, (i + 1)/2);
            } else {
                event = createAcceptEvent(1, prepareReq.instanceId(), prepareReq.squadId(), v);
            }
            events.add(event);
        }
        return events;
    }

    private class JaxosClientHandler extends SimpleChannelInboundHandler {

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.info("Channel closed");
            iJoined = remoteJoined = false;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            JaxosSettings.Peer peer = settings.getPeer(serverId);
            if (peer == null) {
                throw new IllegalArgumentException("Given server id not in config " + serverId);
            }
            Event.JoinRequest req = new Event.JoinRequest(settings.serverId(), settings.connectToken(), settings.self().address(),
                    settings.partitionNumber(), ProtoMessageCoder.MESSAGE_VERSION, settings.appMessageVersion(),
                    peer.address());

            PaxosMessage.DataGram d = coder.encode(req);
            ctx.writeAndFlush(d);

            super.channelActive(ctx);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Object msg) {
            //logger.info("Read msg {}", Objects.toString(msg));

            if (msg instanceof PaxosMessage.DataGram) {
                PaxosMessage.DataGram dataGram = (PaxosMessage.DataGram) msg;
                //this is an empty dataGram
                //TODO ingest why
                if (dataGram.getCode() == PaxosMessage.Code.NONE) {
                    return;
                }

                Event event = coder.decode(dataGram);
                if (event != null) {
                    if(event.code() == Event.Code.JOIN_REQUEST){
                        Event.JoinResponse resp = new Event.JoinResponse(settings.serverId(), true, "come on!");
                        ctx.writeAndFlush(coder.encode(resp));
                        remoteJoined = true;
                        System.out.println("Remote Joined!");
                    } else if (event.code() == Event.Code.JOIN_RESPONSE) {
                        Event.JoinResponse resp = (Event.JoinResponse)coder.decode(dataGram);
                        iJoined = resp.success();
                        System.out.println("I joined!");
                    } else {
                        consumeEvent(event);
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
                logger.warn("Connection reseted by peer");
            }
            else {
                logger.error("error ", cause);
            }
            ctx.close();
        }
    }

    public static void main(String[] args) throws Exception {
        String server =  args[0];
        int amp = Integer.parseInt(args[1]);

        List<Event> events = generateEvents(20_000);
        for(int i = 1; i <= 30; i++) {
            int k = Math.min(i, amp);
            EventSender fetcher = new EventSender(server);

            int times = 200_000 * k;
            fetcher.start(events, 4000 * k, times);

            if(fetcher.writerThread != null){
                fetcher.writerThread.join();
            }

            while(fetcher.receivedCount.get() < times) {
                Thread.sleep(1000);
            }

            fetcher.close();
            System.out.println(fetcher.registry.scrape());

            Thread.sleep(3000);
            System.out.println();
            System.out.println("--------------------------------------");
        }
    }
}
