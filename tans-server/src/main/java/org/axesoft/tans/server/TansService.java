package org.axesoft.tans.server;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.base.Either;
import org.axesoft.jaxos.base.LongRange;
import org.axesoft.tans.protobuff.TansMessage;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TansService implements StateMachine, HasMetrics {
    private static Logger logger = LoggerFactory.getLogger(TansService.class);

    private Supplier<Proponent> proponent;
    private TansConfig config;

    private TansNumberMap[] numberMaps;
    private Object[] machineLocks;
    private PartedThreadPool partedThreadPool;

    public TansService(TansConfig config, Supplier<Proponent> proponent, PartedThreadPool partedThreadPool) {
        this.proponent = checkNotNull(proponent);
        this.config = config;
        this.partedThreadPool = partedThreadPool;
        this.numberMaps = new TansNumberMap[this.config.jaxConfig().partitionNumber()];
        this.machineLocks = new Object[this.config.jaxConfig().partitionNumber()];
        for (int i = 0; i < numberMaps.length; i++) {
            this.numberMaps[i] = new TansNumberMap();
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
    public long currentVersion(int squadId) {
        return this.numberMaps[squadId].currentVersion();
    }

    @Override
    public void consume(int squadId, long instanceId, ByteString proposal) {
        List<TansNumber> nx = proposal.isEmpty() ? Collections.emptyList() : fromProposal(proposal);
        if (logger.isTraceEnabled()) {
            logger.trace("TANS state machine consume {} event from instance {}.{}", nx.size(), squadId, instanceId);
        }
        this.numberMaps[squadId].consume(instanceId, nx);
    }

    @Override
    public Pair<ByteString, Long> makeCheckPoint(int squadId) {
        TansNumberMapSnapShot p = this.numberMaps[squadId].createSnapShot();
        ByteString content = toCheckPoint(p.numbers());
        return Pair.of(content, p.version());
    }

    @Override
    public void restoreFromCheckPoint(int squadId, long version, ByteString checkPoint) {
        List<TansNumber> nx = fromCheckPoint(checkPoint);
        final long instanceId = version;
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
        checkArgument(requests.size() > 0, "requests is empty");

        TansNumberProposal proposal;

        proposal = this.numberMaps[squadId].createProposal(requests);
        ByteString bx = toProposal(proposal.numbers);

        return proponent.get().propose(squadId, bx, ignoreLeader)
                .thenApply(r -> r.map(snapshot ->  produceResult((TansNumberMapSnapShot)snapshot, requests)))
                .exceptionally(ex -> ProposeResult.fail(ex.getClass().getName() + ": " +ex.getMessage()));


//        return Futures.transform(resultFuture, snapshot -> produceResult((TansNumberMapSnapShot)snapshot, requests), this.partedThreadPool.executorOf(squadId));
//
//        try {
//            StateMachine.Snapshot snapshot = resultFuture.get(1, TimeUnit.SECONDS);
//            return produceResult((TansNumberMapSnapShot)snapshot, requests);
//        }
//        catch(TimeoutException e){
//            throw new RuntimeException(e);
//        }
//        catch (InterruptedException e) {
//            //logger.debug("Execution interrupted", e);
//            Thread.currentThread().interrupt();
//            throw new RuntimeException(e);
//        }
//        catch (ExecutionException e) {
//            if (e.getCause() instanceof RuntimeException) {
//                throw (RuntimeException) e.getCause();
//            }
//            else {
//                throw new RuntimeException(e.getCause());
//            }
//        }
    }

    private List<LongRange> produceResult(TansNumberMapSnapShot snapShot, List<KeyLong> requests) {
        PMap<String, TansNumber> numbers = snapShot.numbers;

        ImmutableList.Builder<LongRange> builder = ImmutableList.builder();
        for (int i = requests.size() - 1; i >= 0; i--) {
            KeyLong req = requests.get(i);
            TansNumber n = numbers.get(req.key());

            builder.add(new LongRange(n.value() - req.value(), n.value() - 1));

            numbers = numbers.plus(n.name(), n.increase(-req.value()));
        }

        return builder.build().reverse();
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
        private long lastInstanceId;

        public TansNumberMapSnapShot(PMap<String, TansNumber> numbers, long lastInstanceId) {
            this.numbers = numbers;
            this.lastInstanceId = lastInstanceId;
        }

        public TansNumber get(String key) {
            return numbers.get(key);
        }

        public long version() {
            return this.lastInstanceId;
        }

        public Collection<TansNumber> numbers() {
            return numbers.values();
        }
    }

    private static class TansNumberMap {
        private PMap<String, TansNumber> numbers = HashTreePMap.empty();
        private long lastInstanceId;

        synchronized long currentVersion() {
            return this.lastInstanceId;
        }

        synchronized void learnLastChosenVersion(long instanceId) {
            this.lastInstanceId = instanceId;
        }

        synchronized void consume(long instanceId, List<TansNumber> nx) {
            if (instanceId != this.lastInstanceId + 1) {
                throw new IllegalStateException(String.format("dolog %d when current is %d", instanceId, this.lastInstanceId));
            }

            this.numbers = applyChange(nx, this.numbers);
            this.lastInstanceId = instanceId;
        }

        synchronized public TansNumberMapSnapShot createSnapShot() {
            return new TansNumberMapSnapShot(this.numbers, this.lastInstanceId);
        }

        private PMap<String, TansNumber> applyChange(List<TansNumber> nx, PMap<String, TansNumber> numbers0) {
            PMap<String, TansNumber> numbers1 = numbers0;
            for (TansNumber n1 : nx) {
                TansNumber n0 = numbers1.get(n1.name());
                if (n0 == null) {
                    numbers1 = numbers1.plus(n1.name(), new TansNumber(n1.name(), n1.value() + 1));
                }
                else {
                    numbers1 = numbers1.plus(n0.name(), n0.increase(n1.value()));
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("Statemachine apply change {}", n1);
                }
            }
            return numbers1;
        }

        private TansNumberProposal createProposal(List<KeyLong> requests) {
            ImmutableList.Builder<TansNumber> builder = ImmutableList.builder();
            for (KeyLong k : requests) {
                builder.add(new TansNumber(k.key(), k.value()));
            }
            return new TansNumberProposal(this.lastInstanceId + 1, builder.build());
        }

        synchronized void transFromCheckPoint(long instanceId, List<TansNumber> nx) {
            this.lastInstanceId = instanceId;
            Map<String, TansNumber> m = nx.stream().collect(Collectors.toMap(TansNumber::name, Functions.identity()));
            this.numbers = HashTreePMap.from(m);
        }
    }

    private static class TansNumberProposal {
        final long instanceId;
        final List<TansNumber> numbers;

        public TansNumberProposal(long instanceId, List<TansNumber> numbers) {
            this.instanceId = instanceId;
            this.numbers = numbers;
        }

        @Override
        public String toString() {
            return "TansNumberProposal{" +
                    ", instanceId=" + instanceId +
                    ", numbers=" + numbers +
                    '}';
        }
    }


    private static ByteString toProposal(List<TansNumber> nx) {
        TansMessage.TansProposal.Builder builder = TansMessage.TansProposal.newBuilder();
        for (TansNumber n : nx) {
            TansMessage.ProtoTansNumber.Builder nb = TansMessage.ProtoTansNumber.newBuilder()
                    .setName(n.name())
                    .setValue(n.value())
                    .setVersion(n.version())
                    .setTimestamp(n.timestamp());

            TansMessage.NumberProposal.Builder pb = TansMessage.NumberProposal.newBuilder()
                    .setNumber(nb)
                    .setVersion0(n.version0())
                    .setValue0(n.value0());

            builder.addProposal(pb);
        }

        return builder.build().toByteString();
    }

    private static List<TansNumber> fromProposal(ByteString message) {
        TansMessage.TansProposal proposal;
        try {
            proposal = TansMessage.TansProposal.parseFrom(message);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        if (proposal.getProposalCount() == 0) {
            throw new RuntimeException("Empty tans number list");
        }

        ImmutableList.Builder<TansNumber> builder = ImmutableList.builder();
        for (TansMessage.NumberProposal np : proposal.getProposalList()) {
            TansMessage.ProtoTansNumber n = np.getNumber();
            builder.add(new TansNumber(n.getName(), n.getVersion(), n.getTimestamp(), n.getValue(),
                    np.getVersion0(), np.getValue0()));
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
