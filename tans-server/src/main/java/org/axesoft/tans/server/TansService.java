package org.axesoft.tans.server;

import com.google.common.base.Functions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.base.LongRange;
import org.axesoft.tans.protobuff.TansMessage;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TansService implements StateMachine, HasMetrics {
    public static final String MESSAGE_VERSION = "0.1.5";

    private static Logger logger = LoggerFactory.getLogger(TansService.class);

    private Supplier<Proponent> proponent;
    private TansConfig config;

    private TansNumberMap[] numberMaps;
    private Object[] machineLocks;

    private Cache<Long, LongRange> recentResultCache;
    private Deque<Map<Long, LongRange>> mapPool = new ConcurrentLinkedDeque<>();

    public TansService(TansConfig config, Supplier<Proponent> proponent) {
        this.proponent = checkNotNull(proponent);
        this.config = config;
        this.numberMaps = new TansNumberMap[this.config.jaxConfig().partitionNumber()];

        this.recentResultCache = CacheBuilder.newBuilder()
                .expireAfterAccess(Duration.ofSeconds(5))
                .concurrencyLevel(32)
                .maximumSize(500_000)
                .build();

        this.machineLocks = new Object[this.config.jaxConfig().partitionNumber()];
        for (int i = 0; i < numberMaps.length; i++) {
            this.numberMaps[i] = new TansNumberMap(this.recentResultCache);
            this.machineLocks[i] = new Object();
        }
    }

    public Number keyCountOf(int squadId) {
        if (squadId >= 0 && squadId < numberMaps.length) {
            return numberMaps[squadId].numbers.size();
        }
        return 0;
    }

    @Override
    public void consume(int squadId, long instanceId, ByteString proposal) {
        List<KeyLong> kx = proposal.isEmpty() ? Collections.emptyList() : fromProposal(proposal);
        if (logger.isTraceEnabled()) {
            logger.trace("TANS state machine consume {} event from instance {}.{}", kx.size(), squadId, instanceId);
        }
        this.numberMaps[squadId].consume(instanceId, kx);
    }

    @Override
    public ByteString makeCheckPoint(int squadId) {
        TansNumberMapSnapShot p = this.numberMaps[squadId].createSnapShot();
        return toCheckPoint(p.numbers());
    }

    @Override
    public void restoreFromCheckPoint(int squadId, long instanceId, ByteString checkPoint) {
        List<TansNumber> nx = fromCheckPoint(checkPoint);
        this.numberMaps[squadId].transFromCheckPoint(instanceId, nx);
    }

    @Override
    public Snapshot getSnapshot(int squadId) {
        return this.numberMaps[squadId].createSnapShot();
    }

    @Override
    public void close() {
        logger.info("TANS state machine closed");
    }

    public CompletableFuture<ProposeResult<List<LongRange>>> acquire(int squadId, List<KeyLong> requests, boolean ignoreLeader) {
        if (requests.size() == 0) {
            return CompletableFuture.completedFuture(ProposeResult.success(ImmutableList.of()));
        }
        Map<Long, LongRange> m = mapPool.poll();
        Map<Long, LongRange> containedResults = m == null ? new HashMap<>() : m;
        containedResults.clear();

        ImmutableList.Builder<KeyLong> builder = ImmutableList.builder();

        for (KeyLong req : requests) {
            LongRange r = this.recentResultCache.getIfPresent(req.stamp());
            if (r != null) {
                containedResults.put(req.stamp(), r);
            }
            else {
                builder.add(req);
            }
        }

        List<KeyLong> newRequests = containedResults.size() == 0 ? requests : builder.build();

        if (newRequests.size() > 0) {
            ByteString bx = toProposal(newRequests);

            return proponent.get().propose(squadId, bx, ignoreLeader)
                    .thenApply(r -> r.map(snapshot -> produceResult((TansNumberMapSnapShot) snapshot, containedResults, requests)))
                    .exceptionally(ex -> {
                        logger.error("at TansService.acquire", ex);
                        return ProposeResult.fail(ex.getClass().getName() + ": " + ex.getMessage());
                    });
        }
        else {
            return CompletableFuture.completedFuture(ProposeResult.success(produceResult(null, containedResults, requests)));
        }
    }

    public CompletableFuture<ProposeResult<Integer>> acquireClientId(){
        return CompletableFuture.completedFuture(ProposeResult.success(100));
    }

    /**
     * Read the value from local state. This is not a consensus read
     *
     * @param key the key name
     * @return value if key exist
     */
    public Optional<TansNumber> localValueOf(String key) {
        checkNotNull(key);
        int squadId = squadIdOf(key);
        return Optional.ofNullable(this.numberMaps[squadId].numbers.get(key));
    }

    private List<LongRange> produceResult(TansNumberMapSnapShot snapShot, Map<Long, LongRange> containedResults, List<KeyLong> requests) {
        try {
            ImmutableList.Builder<LongRange> builder = ImmutableList.builder();
            for (KeyLong req : requests) {
                LongRange r = snapShot == null? null : snapShot.resultCache().getIfPresent(req.stamp());
                if (r == null) {
                    r = containedResults.get(req.stamp());
                }
                if (r == null) {
                    logger.error("No result found for req {}", req.stamp());
                    r = new LongRange(0, 0);
                }

                builder.add(r);
            }
            return builder.build();
        }
        finally {
            this.mapPool.offerLast(containedResults);
        }
    }

    public int squadIdOf(String key) {
        int code = key.hashCode();
        if (code == Integer.MIN_VALUE) {
            return 0;
        }
        return Math.abs(code) % this.numberMaps.length;
    }

    @Override
    public String formatMetrics() {
        return this.proponent.get().formatMetrics();
    }

    private static class TansNumberMapSnapShot implements StateMachine.Snapshot {
        private PMap<String, TansNumber> numbers;
        private long version;
        private Cache<Long, LongRange> resultCache;

        public TansNumberMapSnapShot(PMap<String, TansNumber> numbers, long version, Cache<Long, LongRange> resultCache) {
            this.numbers = numbers;
            this.version = version;
            this.resultCache = resultCache;
        }

        public TansNumber get(String key) {
            return numbers.get(key);
        }

        public long version() {
            return this.version;
        }

        public Collection<TansNumber> numbers() {
            return numbers.values();
        }

        public Cache<Long, LongRange> resultCache() {
            return this.resultCache;
        }
    }

    private static class TansNumberMap {
        private PMap<String, TansNumber> numbers = HashTreePMap.empty();
        private long lastInstanceId;
        private Cache<Long, LongRange> recentResultCache;

        public TansNumberMap(Cache<Long, LongRange> recentResultCache) {
            this.recentResultCache = recentResultCache;
        }

        synchronized long currentVersion() {
            return this.lastInstanceId;
        }

        synchronized void learnLastChosenVersion(long instanceId) {
            this.lastInstanceId = instanceId;
        }

        synchronized void consume(long instanceId, List<KeyLong> kx) {
            if (instanceId <= this.lastInstanceId) {
                throw new IllegalStateException(String.format("dolog %d when current is %d", instanceId, this.lastInstanceId));
            }

            this.numbers = applyChange(kx, this.numbers);
            this.lastInstanceId = instanceId;
        }

        synchronized public TansNumberMapSnapShot createSnapShot() {
            return new TansNumberMapSnapShot(this.numbers, this.lastInstanceId, this.recentResultCache);
        }

        private PMap<String, TansNumber> applyChange(List<KeyLong> kx, PMap<String, TansNumber> numbers0) {
            PMap<String, TansNumber> numbers1 = numbers0;
            for (KeyLong k : kx) {
                TansNumber n0 = numbers1.get(k.key());
                TansNumber n1;
                if (n0 == null) {
                    n1 = new TansNumber(k.key(), k.value() + 1);
                }
                else {
                    n1 = n0.increase(k.value());
                }
                numbers1 = numbers1.plus(k.key(), n1);

                this.recentResultCache.put(k.stamp(), new LongRange(n1.value() - k.value(), n1.value() - 1, n1.timestamp()));

                if (logger.isTraceEnabled()) {
                    logger.trace("StateMachine apply change {}", k);
                }
            }
            return numbers1;
        }

        synchronized void transFromCheckPoint(long instanceId, List<TansNumber> nx) {
            this.lastInstanceId = instanceId;
            Map<String, TansNumber> m = nx.stream().collect(Collectors.toMap(TansNumber::name, Functions.identity()));
            this.numbers = HashTreePMap.from(m);
        }
    }


    private static ByteString toProposal(List<KeyLong> kx) {
        TansMessage.AcquireNumberProposal.Builder builder = TansMessage.AcquireNumberProposal.newBuilder();

        for (KeyLong k : kx) {
            TansMessage.NumberProposal.Builder b = TansMessage.NumberProposal.newBuilder()
                    .setName(k.key())
                    .setValue(k.value())
                    .setSequence(k.stamp());

            builder.addProposal(b);
        }

        return TansMessage.TansProposal.newBuilder()
                .setType(TansMessage.ProposalType.ACQUIRE_NUMBER)
                .setContent(builder.build().toByteString())
                .build().toByteString();
    }

    private static List<KeyLong> fromProposal(ByteString message) {
        TansMessage.AcquireNumberProposal np;
        try {
            TansMessage.TansProposal proposal = TansMessage.TansProposal.parseFrom(message);
            if(proposal.getType() != TansMessage.ProposalType.ACQUIRE_NUMBER){
                throw new UnsupportedOperationException();
            }
            np = TansMessage.AcquireNumberProposal.parseFrom(proposal.getContent());
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }



        ImmutableList.Builder<KeyLong> builder = ImmutableList.builder();
        for (TansMessage.NumberProposal p : np.getProposalList()) {
            builder.add(new KeyLong(p.getName(), p.getValue(), p.getSequence()));
        }

        return builder.build();
    }


    private static ByteString toCheckPoint(Collection<TansNumber> nx) {
        TansMessage.TansCheckPoint.Builder cb = TansMessage.TansCheckPoint.newBuilder();

        for (TansNumber n : nx) {
            TansMessage.ProtoTansNumber.Builder nb = TansMessage.ProtoTansNumber.newBuilder()
                    .setName(n.name())
                    .setValue(n.value())
                    .setVersion(n.version())
                    .setTimestamp(n.timestamp());

            cb.addNumber(nb);
        }

        return cb.build().toByteString();
    }

    private static List<TansNumber> fromCheckPoint(ByteString content) {
        TansMessage.TansCheckPoint checkPoint;
        try {
            checkPoint = TansMessage.TansCheckPoint.parseFrom(content);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        return checkPoint.getNumberList().stream()
                .map(n -> new TansNumber(n.getName(), n.getVersion(), n.getTimestamp(), n.getValue()))
                .collect(Collectors.toList());
    }
}
