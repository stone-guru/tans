package org.axesoft.jaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.Range;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.base.GroupedRateLimiter;
import org.axesoft.jaxos.base.NumberedThreadFactory;
import org.axesoft.jaxos.logger.MemoryAcceptorLogger;
import org.axesoft.jaxos.logger.RocksDbAcceptorLogger;
import org.axesoft.jaxos.algo.EventWorkerPool;
import org.axesoft.jaxos.netty.NettyCommunicatorFactory;
import org.axesoft.jaxos.netty.NettyJaxosServer;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JaxosService extends AbstractExecutionThreadService implements Proponent {
    private static final String SERVICE_NAME = "Jaxos service";

    private static final Logger logger = LoggerFactory.getLogger(JaxosService.class);
    private static final Duration LEADER_CHECK_DURATION = Duration.ofMillis(100);

    private JaxosSettings settings;
    private StateMachine stateMachine;
    private AcceptorLogger acceptorLogger;
    private Communicator communicator;
    private NettyJaxosServer node;

    private EventWorkerPool eventWorkerPool;
    private Squad[] squads;
    private Platoon platoon;

    private ScheduledExecutorService timerExecutor;
    private Components components;
    private MessageIdHolder messageIdHolder;
    private JaxosMetrics jaxosMetrics;
    private SquadSelector checkPointSquadSelector;

    public JaxosService(JaxosSettings settings, StateMachine stateMachine) {
        checkArgument(settings.partitionNumber() > 0, "Invalid partition number %d", settings.partitionNumber());

        this.settings = checkNotNull(settings, "The param settings is null");
        this.stateMachine = checkNotNull(stateMachine, "The param stateMachine is null");
        this.messageIdHolder = new MessageIdHolder(this.settings.serverId());
        this.jaxosMetrics = new MicroMeterJaxosMetrics(this.settings.serverId());
        this.acceptorLogger = createLoggerBackend(this.settings, this.jaxosMetrics);

        this.components = new Components() {
            @Override
            public Communicator getCommunicator() {
                return JaxosService.this.communicator;
            }

            @Override
            public AcceptorLogger getLogger() {
                return JaxosService.this.acceptorLogger;
            }

            @Override
            public EventWorkerPool getWorkerPool() {
                return JaxosService.this.eventWorkerPool;
            }

            @Override
            public EventTimer getEventTimer() {
                return JaxosService.this.eventWorkerPool;
            }

            @Override
            public JaxosMetrics getJaxosMetrics() {
                return JaxosService.this.jaxosMetrics;
            }
        };


        this.squads = new Squad[settings.partitionNumber()];
        for (int i = 0; i < settings.partitionNumber(); i++) {
            this.squads[i] = new Squad(i, settings, this.components, stateMachine);
        }
        this.checkPointSquadSelector = new SquadSelector(settings.partitionNumber(), Duration.ofMinutes(settings.checkPointMinutes()));
        this.platoon = new Platoon();
        this.eventWorkerPool = new EventWorkerPool(settings.algoThreadNumber(), () -> this.platoon);


        this.timerExecutor = Executors.newScheduledThreadPool(2, new NumberedThreadFactory("JaxosScheduledTask"));

        super.addListener(new JaxosServiceListener(), MoreExecutors.directExecutor());

        restoreFromDb();
    }

    private AcceptorLogger createLoggerBackend(JaxosSettings settings, JaxosMetrics metrics) {
        if ("rocksdb".equals(settings.loggerBackend())) {
            return new RocksDbAcceptorLogger(settings.dbDirectory(), settings.partitionNumber(),
                    settings.syncInterval().toMillis() < 100, metrics);
        }
        else if ("memory".equals(settings.loggerBackend())) {
            metrics.setLoggerDiskSizeSupplierIfNot(() -> 0L);
            return new MemoryAcceptorLogger(25000);
        }
        else {
            throw new IllegalArgumentException("Unknown logger backend '" + settings.loggerBackend() + "'");
        }

    }

    private void restoreFromDb() {
        if (settings.loggerBackend().equals("memory")) {
            logger.info("initialize in memory DB");
        }
        else {
            logger.info("Restore from DB at {}", settings.dbDirectory());
        }

        long t0 = System.currentTimeMillis();

        AtomicReference<Boolean> errorOccur = new AtomicReference<>(false);
        CountDownLatch latch = new CountDownLatch(settings.partitionNumber());
        for (int i = 0; i < settings.partitionNumber(); i++) {
            final int n = i;
            this.eventWorkerPool.queueTask(n, () -> {
                try {
                    this.squads[n].restoreFromDB();
                }
                catch (Exception e) {
                    errorOccur.set(true);
                    logger.error("S" + n + " error when restoreFromDB", e);
                }
                finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            if (errorOccur.get()) {
                throw new RuntimeException("RestoreFromDB failed");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        long elapsed = System.currentTimeMillis() - t0;
        this.jaxosMetrics.recordRestoreElapsedMillis(elapsed);
        logger.info("Restored from DB in {} sec", String.format("%.3f", elapsed / 1000.0));
    }

    @Override
    public CompletableFuture<ProposeResult<StateMachine.Snapshot>> propose(int squadId, ByteString v, boolean ignoreLeader) {
        long ballotId = messageIdHolder.nextIdOf(squadId);
        return propose(squadId, new Event.BallotValue(ballotId, Event.ValueType.APPLICATION, v), ignoreLeader);
    }

    private CompletableFuture<ProposeResult<StateMachine.Snapshot>> propose(int squadId, Event.BallotValue v, boolean ignoreLeader) {
        if (!this.isRunning()) {
            return CompletableFuture.completedFuture(ProposeResult.fail(SERVICE_NAME + " is not running"));
        }

        checkArgument(squadId >= 0 && squadId < squads.length,
                "Invalid squadId(%s) while partition number is %s ", squadId, squads.length);

        CompletableFuture<ProposeResult<StateMachine.Snapshot>> resultFuture = new CompletableFuture<>();
        this.squads[squadId].propose(v, ignoreLeader, resultFuture);

        return resultFuture;
    }

    @Override
    protected void triggerShutdown() {
        ImmutableList<Runnable> closeActions = ImmutableList.of(
                this.timerExecutor::shutdownNow,
                this.communicator::close,
                this.node::shutdown,
                this.stateMachine::close,
                this.acceptorLogger::close,
                this.eventWorkerPool::shutdown);

        for (Runnable r : closeActions) {
            try {
                r.run();
            }
            catch (Exception e) {
                logger.error("Error when shutdown", e);
            }
        }
    }

    @Override
    protected void run() throws Exception {
        if (settings.syncInterval().toMillis() >= 100) {
            this.timerExecutor.scheduleWithFixedDelay(() -> new RunnableWithLog(logger, components.getLogger()::sync).run(),
                    settings.syncInterval().toMillis(), settings.syncInterval().toMillis(), TimeUnit.MILLISECONDS);
        }
        else {
            logger.info("Regard sync interval of " + settings.syncInterval() + " as always do sync ");
        }

        this.timerExecutor.scheduleWithFixedDelay(this::checkAndSaveCheckPoint, settings.checkPointMinutes() * 60, 10, TimeUnit.SECONDS);

        //this.timerExecutor.scheduleWithFixedDelay(this::runForLeader, (this.settings.leaderLeaseSeconds() + 2) * 1000, LEADER_CHECK_DURATION.toMillis(), TimeUnit.MILLISECONDS);


        NettyCommunicatorFactory factory = new NettyCommunicatorFactory(settings, this.eventWorkerPool, this.platoon::createConnectRequest);
        this.communicator = factory.createCommunicator();

        new Thread(() -> {
            try {
                this.confirmPrelifeLastInstanceRouting();
            }
            catch (InterruptedException e) {
                logger.error("Prelife confirming routing interrupted");
            }
        }, "PrelifeLastConfirming").start();

        this.node = new NettyJaxosServer(this.settings, this.eventWorkerPool);
        this.node.start();
    }

    public ScheduledExecutorService timerExecutor() {
        return this.timerExecutor;
    }

    private void confirmPrelifeLastInstanceRouting() throws InterruptedException {
        GroupedRateLimiter rateLimiter = new GroupedRateLimiter(1.0 / 10.0);
        Thread.sleep(1000);

        while (true) {
            boolean remain = false;
            for (Squad squad : squads) {
                if (squad.isPrelifeLastConfirmed()) {
                    continue;
                }
                Instance i = squad.prelifeLastInstance();
                CompletableFuture<ProposeResult<StateMachine.Snapshot>> future = new CompletableFuture<>();
                squad.directPropose(i.id(), i.value(), future);
                future.thenAccept(result -> {
                    switch (result.code()) {
                        case SUCCESS:
                            try {
                                squad.consume(i);
                            }
                            catch (IllegalStateException e) {

                            }
                            logger.info("S{} The prelive last {} confirmed", i, i.squadId());
                            break;
                        default: {
                            if (rateLimiter.tryAcquireFor(squad.id())) {
                                logger.info("Confirm prelife last instance result in {}", result.errorMessage());
                            }
                        }
                    }
                });
                remain = true;
            }

            if (remain) {
                Thread.sleep(200);
            }
            else {
                logger.info("Confirm prelife last finished for all squad");
                break;
            }
        }
    }

    private void runForLeader() {
        long current = System.currentTimeMillis();
        LeaderStatus status = collectLeaderStatus();
        List<Integer> mySquads = status.squadsOf(settings.serverId());
        for (int squadId : mySquads) {
            if (current - squads[squadId].context().chosenTimestamp() >= this.settings.leaderLeaseSeconds() * 1000 - LEADER_CHECK_DURATION.toMillis()) {
                proposeForLeader(squadId);
            }
        }

        Range<Integer> squadCountRange = calcResponsibility(status.activePeerCount(this.settings.serverId()));

        Iterator<Integer> it = status.getCandidates(settings.serverId(), squadCountRange.getMaximum()).iterator();
        int i = mySquads.size();
        while (i < squadCountRange.getMaximum() && it.hasNext()) {
            int squadId = it.next();
            //logger.info("S{} compete for leader", squadId);
            if (this.squads[i].isPrelifeLastConfirmed()) {
                proposeForLeader(squadId);
            }
            i++;
        }
    }

    private LeaderStatus collectLeaderStatus() {
        LeaderStatus status = new LeaderStatus();

        long current = System.currentTimeMillis();
        for (int i = 0; i < this.squads.length; i++) {
            SquadContext context = squads[i].context();
            int serverId = context.lastProposer();
            if (serverId != 0 && context.isLeaderLeaseExpired(current)) {
                //logger.info("S{} Older leader {} expired", i, serverId);
                status.recordSquad(i, 0);
            }
            else {
                status.recordSquad(i, serverId);
            }
        }

        return status;
    }

    private Range<Integer> calcResponsibility(int activePeerCount) {
        int avg = this.squads.length / activePeerCount;
        int extra = this.squads.length - (avg * activePeerCount) > 0 ? 1 : 0;

        return Range.between(avg, avg + extra);
    }

    private void proposeForLeader(int squadId) {
        long ballotId = messageIdHolder.nextIdOf(squadId);
        Event.BallotValue v = new Event.BallotValue(ballotId, Event.ValueType.NOTHING, ByteString.EMPTY);
        this.propose(squadId, v, true)
                .thenAccept(result -> {
                    if (logger.isDebugEnabled()) {
                        if (result.isSuccess()) {
                            logger.debug("S{} got leader again", squadId);
                        }
                        else {
                            logger.debug("S{} propose for leader failed");
                        }
                    }
                })
                .exceptionally(ex -> {
                    if (logger.isDebugEnabled()) {
                        Throwable cause = ex.getCause() == null ? ex : ex.getCause();
                        logger.debug("S{} Failed to be leader due to '{} {}'", squadId, cause, cause.getMessage());
                    }
                    return null;
                });
    }

    private void checkAndSaveCheckPoint() {
        this.checkPointSquadSelector.selectOne(System.currentTimeMillis()).ifPresent(id -> {
            try {
                this.squads[id].saveCheckPoint();
            }
            catch (Exception e) {
                if (e.getCause() instanceof InterruptedException) {
                    logger.info("Save checkpoint S{} interrupted", id);
                    return;
                }
                logger.error("S" + id + " save checkpoint error", e);
            }

        });
    }

    @Override
    public String formatMetrics() {
        return this.jaxosMetrics.format();
    }

    private class Platoon implements EventDispatcher {

        private volatile BitSet connectedPeers;
        private GroupedRateLimiter rateLimiter;

        public Platoon() {
            this.connectedPeers = new BitSet();
            //Itself is always connected, otherwise self-generated event can't be processed
            this.connectedPeers.set(settings.serverId());
            this.rateLimiter = new GroupedRateLimiter(1.0 / 10.0);
        }

        @Override
        public Event processEvent(Event event) {
            if (!state().equals(State.RUNNING)) {
                if (this.rateLimiter.tryAcquireFor(event.senderId())) {
                    logger.info("Abandon events from {} due to not running", event.senderId());
                }
                return null;
            }

            switch (event.code()) {
                case JOIN_REQUEST:
                    return processJoinRequest((Event.JoinRequest) event);
                case PEER_LEFT: {
                    onPeerLeft((Event.PeerLeft) event);
                    return null;
                }
                default: {
                    if (!connectedPeers.get(event.senderId())) {
                        if (rateLimiter.tryAcquireFor(event.senderId())) {
                            logger.info("Abandon events from {} due to not joined", event.senderId());
                        }
                        return null;
                    }
                    return getSquad(event).processEvent(event);
                }
            }
        }

        private Squad getSquad(Event event) {
            final int squadId = event.squadId();
            if (squadId >= 0 && squadId < JaxosService.this.squads.length) {
                return JaxosService.this.squads[squadId];
            }
            else {
                throw new IllegalArgumentException("Invalid squadId in " + event.toString());
            }
        }

        private synchronized Event.JoinResponse processJoinRequest(Event.JoinRequest req) {
            boolean success = false;
            String message = null;

            if (connectedPeers.get(req.senderId())) { //Self id is always in connectedPeers
                message = "another same id(" + req.senderId() + ") server already connected";
            }
            else {
                JaxosSettings.Peer peer = settings.getPeer(req.senderId());
                if (peer == null) {
                    message = String.format("Server id(%d) not found in my settings", req.senderId());
                }
                else {
                    List<Supplier<String>> checks = ImmutableList.of(
                            () -> checkEquals(settings.connectToken(), req.token(), "token"),
                            () -> checkEquals(settings.partitionNumber(), req.partitionNumber(), "partition number"),
                            () -> checkEquals(ProtoMessageCoder.MESSAGE_VERSION, req.jaxosMessageVersion(), "jaxos message version"),
                            () -> checkEquals(settings.appMessageVersion(), req.appMessageVersion(), "app message version"),
                            () -> checkEquals(peer.address(), req.hostname(), "peer hostname"),
                            () -> checkEquals(settings.self().address(), req.destHostname(), "destination server name"));
                    Iterator<Supplier<String>> it = checks.iterator();
                    while (message == null && it.hasNext()) {
                        message = it.next().get();
                    }

                    if (message == null) {
                        BitSet s1 = BitSet.valueOf(this.connectedPeers.toByteArray());
                        s1.set(req.senderId());
                        this.connectedPeers = s1;

                        success = true;
                        message = "ok";
                        logger.info("Server {} joined", req.senderId());
                    }
                }
            }

            return new Event.JoinResponse(settings.serverId(), success, message);
        }

        private String checkEquals(Object mine, Object other, String itemName) {
            if (!Objects.equals(mine, other)) {
                String fmt = mine instanceof String ?
                        "given %s'%s' is unequal to mine'%s'" :
                        "given %s(%s) s unequal to mine(%s)";
                return String.format(fmt, itemName, Objects.toString(other), Objects.toString(mine));
            }
            return null;
        }

        private Event.JoinRequest createConnectRequest(int serverId) {
            JaxosSettings.Peer peer = settings.getPeer(serverId);
            if (peer == null) {
                throw new IllegalArgumentException("Given server id not in config " + serverId);
            }

            return new Event.JoinRequest(settings.serverId(), settings.connectToken(), settings.self().address(),
                    settings.partitionNumber(), ProtoMessageCoder.MESSAGE_VERSION, settings.appMessageVersion(),
                    peer.address());
        }

        private synchronized void onPeerLeft(Event.PeerLeft event) {
            if (this.connectedPeers.get(event.peerId())) {
                BitSet s1 = BitSet.valueOf(this.connectedPeers.toByteArray());
                s1.clear(event.peerId());
                this.connectedPeers = s1;
                logger.info("Server {} left ", event.peerId());
            }
        }
    }

    private class JaxosServiceListener extends Listener {
        @Override
        public void running() {
            logger.info("{} {} started at port {}", SERVICE_NAME, settings.serverId(), settings.self().port());
        }

        @Override
        public void stopping(State from) {
            logger.info("{} {} is stopping", SERVICE_NAME, settings.serverId());
        }

        @Override
        public void terminated(State from) {
            logger.info("{} {} terminated from {}", SERVICE_NAME, settings.serverId(), from);
        }
    }

    private static class LeaderStatus {
        private Map<Integer, List<Integer>> leaderSquadCountMap = new HashMap<>();

        public void recordSquad(int squadId, int serverId) {
            leaderSquadCountMap.compute(serverId, (i, sx0) -> sx0 == null ? new ArrayList<>() : sx0)
                    .add(squadId);
        }

        public List<Integer> squadsOf(int serverId) {
            return leaderSquadCountMap.getOrDefault(serverId, Collections.emptyList());
        }

        public int activePeerCount(int selfId) {
            Set<Integer> s = new TreeSet<>(leaderSquadCountMap.keySet());
            s.remove(0);
            s.add(selfId);
            return s.size();
        }

        public List<Integer> getCandidates(int selfId, int maxSquads) {
            ImmutableList.Builder<Integer> builder = ImmutableList.builder();
            builder.addAll(squadsOf(0));//no leader squads
            for (Map.Entry<Integer, List<Integer>> entry : this.leaderSquadCountMap.entrySet()) {
                int count = entry.getValue().size();
                if (entry.getKey() != 0 && entry.getKey() != selfId && count > maxSquads) {
                    Iterator<Integer> it = entry.getValue().iterator();
                    for (int i = 0; i < count - maxSquads; i++) {
                        builder.add(it.next());
                    }
                }
            }
            return builder.build();
        }
    }

    public static class MessageIdHolder {

        private static class Item {
            private final long highBits;
            private final AtomicInteger serialNum;

            public Item(int highBits) {
                this.highBits = ((long) highBits) << 32;
                this.serialNum = new AtomicInteger(0);
            }

            public long nextId() {
                return this.highBits | (this.serialNum.getAndIncrement() & 0xffffffffL);
            }
        }

        private int serverId;
        private Map<Integer, Item> itemMap = new ConcurrentHashMap<>();

        public MessageIdHolder(int serverId) {
            this.serverId = serverId;
        }

        public long nextIdOf(int squadId) {
            Item item = itemMap.computeIfAbsent(squadId, i -> new Item((serverId << 16) | squadId));
            return item.nextId();
        }
    }

    private static class SquadSelector {
        private Duration interval;
        private int squadCount;
        private Map<Integer, Long> lastSelectMap = new HashMap<>();

        public SquadSelector(int squadCount, Duration interval) {
            this.interval = interval;
            this.squadCount = squadCount;
        }

        public synchronized Optional<Integer> selectOne(long current) {
            for (int i = 0; i < squadCount; i++) {
                Long lastStamp = lastSelectMap.get(i);
                if (lastStamp == null || (current - lastStamp >= interval.toMillis())) {
                    lastSelectMap.put(i, current);
                    return Optional.of(i);
                }
            }
            return Optional.empty();
        }
    }
}
