package org.axesoft.jaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import com.google.protobuf.ByteString;
import io.netty.util.Timeout;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.logger.MemoryAcceptorLogger;
import org.axesoft.jaxos.logger.RocksDbAcceptorLogger;
import org.axesoft.jaxos.algo.EventWorkerPool;
import org.axesoft.jaxos.netty.NettyCommunicatorFactory;
import org.axesoft.jaxos.netty.NettyJaxosServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

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
    private RequestExecutor requestExecutor;

    private EventWorkerPool eventWorkerPool;
    private Squad[] squads;
    private Platoon platoon;

    private ScheduledExecutorService timerExecutor;
    private Components components;
    private BallotIdHolder ballotIdHolder;
    private JaxosMetrics jaxosMetrics;
    private SquadSelector checkPointSquadSelector;

    public JaxosService(JaxosSettings settings, StateMachine stateMachine, RequestExecutor requestExecutor) {
        checkArgument(settings.partitionNumber() > 0, "Invalid partition number %d", settings.partitionNumber());

        this.settings = checkNotNull(settings, "The param settings is null");
        this.stateMachine = checkNotNull(stateMachine, "The param stateMachine is null");
        this.ballotIdHolder = new BallotIdHolder(this.settings.serverId());
        this.jaxosMetrics = new MicroMeterJaxosMetrics(this.settings.serverId());

        if ("rocksdb".equals(settings.loggerImplementation())) {
            this.acceptorLogger = new RocksDbAcceptorLogger(this.settings.dbDirectory(), this.settings.partitionNumber(),
                    this.settings.syncInterval().toMillis() < 100, jaxosMetrics);
        }
        else if ("memory".equals(settings.loggerImplementation())) {
            this.acceptorLogger = new MemoryAcceptorLogger(25000);
        }
        else {
            throw new IllegalArgumentException("Unknown logger implementation '" + settings.loggerImplementation() + "'");
        }

        this.requestExecutor = requestExecutor;

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


        this.timerExecutor = Executors.newScheduledThreadPool(2, (r) -> {
            String name = "scheduledTaskThread";
            Thread thread = new Thread(r, name);
            thread.setDaemon(true);
            return thread;
        });


        super.addListener(new JaxosServiceListener(), MoreExecutors.directExecutor());

        restoreFromDb();
    }

    private void restoreFromDb() {
        if ("memory".equals(settings.loggerImplementation())) {
            logger.info("Initialize in memory log db");
            return;
        }

        logger.info("Restore from DB at {}", settings.dbDirectory());

        long t0 = System.currentTimeMillis();

        AtomicReference<Exception> exceptionRef = new AtomicReference<>(null);
        CountDownLatch latch = new CountDownLatch(settings.partitionNumber());
        for (int i = 0; i < settings.partitionNumber(); i++) {
            final int n = i;
            this.eventWorkerPool.queueTask(n, () -> {
                try {
                    this.squads[n].restoreFromDB();
                }
                catch (Exception e) {
                    exceptionRef.set(e);
                }
                finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            if (exceptionRef.get() != null) {
                throw new RuntimeException(exceptionRef.get());
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        long elapsed = System.currentTimeMillis() - t0;
        this.jaxosMetrics.recordRestoreElapsedMillis(elapsed);
        logger.info("Restored from DB in {} sec", String.format("%.3f", elapsed / 1000.0));
    }

    @Override
    public CompletableFuture<ProposeResult<StateMachine.Snapshot>> propose(int squadId, ByteString v, boolean ignoreLeader) {
        long ballotId = ballotIdHolder.nextIdOf(squadId);
        return propose(squadId, new Event.BallotValue(ballotId, Event.ValueType.APPLICATION, v), ignoreLeader);
    }

    private CompletableFuture<ProposeResult<StateMachine.Snapshot>> propose(int squadId, Event.BallotValue v, boolean ignoreLeader) {
        if (!this.isRunning()) {
            return CompletableFuture.completedFuture(ProposeResult.fail(SERVICE_NAME + " is not running"));
        }

        checkArgument(squadId >= 0 && squadId < squads.length,
                "Invalid squadId(%s) while partition number is %s ", squadId, squads.length);

        CompletableFuture<ProposeResult<StateMachine.Snapshot>> resultFuture = new CompletableFuture<>();
        this.eventWorkerPool.queueTask(squadId, () -> this.squads[squadId].propose(v, ignoreLeader, resultFuture));

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
        this.node = new NettyJaxosServer(this.settings, this.eventWorkerPool);

        NettyCommunicatorFactory factory = new NettyCommunicatorFactory(settings, this.eventWorkerPool);
        this.communicator = factory.createCommunicator();

        if (settings.syncInterval().toMillis() >= 100) {
            this.timerExecutor.scheduleWithFixedDelay(() -> new RunnableWithLog(logger, components.getLogger()::sync).run(),
                    settings.syncInterval().toMillis(), settings.syncInterval().toMillis(), TimeUnit.MILLISECONDS);
        }
        else {
            logger.info("Regard sync interval of " + settings.syncInterval() + " as always do sync ");
        }

        this.timerExecutor.scheduleWithFixedDelay(this::checkAndSaveCheckPoint, settings.checkPointMinutes() * 60, 10, TimeUnit.SECONDS);

        this.timerExecutor.scheduleWithFixedDelay(this::runForLeader, (this.settings.leaderLeaseSeconds() + 2) * 1000, LEADER_CHECK_DURATION.toMillis(), TimeUnit.MILLISECONDS);

        this.node.startup();
    }

    public ScheduledExecutorService timerExecutor() {
        return this.timerExecutor;
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
            proposeForLeader(squadId);
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
        if (logger.isDebugEnabled()) {
            logger.debug("S{} propose for leader", squadId);
        }

        //this.requestExecutor.submit(squadId, () -> execProposeForLeader(squadId));
        this.eventWorkerPool.submitBackendTask(() -> execProposeForLeader(squadId));
    }

    private void execProposeForLeader(int squadId) {
        long ballotId = ballotIdHolder.nextIdOf(squadId);
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
        /**
         * Map(SquadId, Pair(ServerId, last chosen instance id))
         */
        private Map<Integer, Pair<Integer, Long>> squadInstanceMap = new HashMap<>();
        private int chosenQueryResponseCount = 0;
        private Timeout chosenQueryTimeout;

        @Override
        public Event processEvent(Event event) {
            switch (event.code()) {
                case CHOSEN_QUERY: {
                    return makeChosenQueryResponse();
                }
                case CHOSEN_QUERY_RESPONSE: {
                    onChosenQueryResponse((Event.ChosenQueryResponse) event);
                    if (this.chosenQueryResponseCount == settings.peerCount() - 1) {
                        if (chosenQueryTimeout != null) {
                            chosenQueryTimeout.cancel();
                        }
                        endChosenQueryResponse();
                    }
                    return null;
                }
                case CHOSEN_QUERY_TIMEOUT: {
                    endChosenQueryResponse();
                    return null;
                }
                default: {
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

        private Event.ChosenQueryResponse makeChosenQueryResponse() {
            ImmutableList.Builder<Pair<Integer, Long>> builder = ImmutableList.builder();
            for (int i = 0; i < JaxosService.this.squads.length; i++) {
                builder.add(Pair.of(i, JaxosService.this.squads[i].lastChosenInstanceId()));
            }
            Event.ChosenQueryResponse r = new Event.ChosenQueryResponse(settings.serverId(), builder.build());

            if (logger.isTraceEnabled()) {
                logger.trace("Generate {}", r);
            }
            return r;
        }


        private void onChosenQueryResponse(Event.ChosenQueryResponse response) {
            this.chosenQueryResponseCount++;
            for (Pair<Integer, Long> p : response.squadChosen()) {
                this.squadInstanceMap.merge(p.getKey(), Pair.of(response.senderId(), p.getRight()),
                        (p0, p1) -> {
                            if (p0.getRight() >= p1.getRight()) {
                                return p0;
                            }
                            else {
                                return p1;
                            }
                        });
            }
        }

        private void endChosenQueryResponse() {
            for (Map.Entry<Integer, Pair<Integer, Long>> entry : squadInstanceMap.entrySet()) {
                int squadId = entry.getKey();
                int serverId = entry.getValue().getKey();
                long instanceId = entry.getValue().getValue();

                Pair<Integer, Long> p = Pair.of(squadId, instanceId);
                Event.ChosenQueryResponse response = new Event.ChosenQueryResponse(serverId, ImmutableList.of(p));
                components.getWorkerPool().queueTask(0, () ->
                        JaxosService.this.squads[squadId].processEvent(response));
            }
        }
    }

    private class JaxosServiceListener extends Listener {
        @Override
        public void running() {
            logger.info("{} {} started at port {}", SERVICE_NAME, settings.serverId(), settings.self().port());
            //logger.info("Using {} ", settings);
        }

        @Override
        public void stopping(State from) {
            logger.info("{} is stopping", SERVICE_NAME);
        }

        @Override
        public void terminated(State from) {
            logger.info("{} terminated from {}", SERVICE_NAME, from);
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

    public static class BallotIdHolder {

        private static class Item {
            private long highBits;
            private int serialNum;

            public Item(int highBits) {
                this.highBits = ((long) highBits) << 32;
                this.serialNum = 0;
            }

            public synchronized long nextId() {
                long r = this.highBits | (this.serialNum & 0xffffffffL);
                this.serialNum++;
                return r;
            }
        }

        private int serverId;
        private Map<Integer, Item> itemMap = new ConcurrentHashMap<>();

        public BallotIdHolder(int serverId) {
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
