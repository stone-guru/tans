package org.axesoft.tans.server;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.algo.*;
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

    public TansService(TansConfig config, Supplier<Proponent> proponent) {
        this.proponent = checkNotNull(proponent);
        this.config = config;
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
        List<KeyLong> kx = proposal.isEmpty() ? Collections.emptyList() : fromProposal(proposal);
        if (logger.isTraceEnabled()) {
            logger.trace("TANS state machine consume {} event from instance {}.{}", kx.size(), squadId, instanceId);
        }
        this.numberMaps[squadId].consume(instanceId, kx);
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
        if(requests.size() == 0){
            return CompletableFuture.completedFuture(ProposeResult.success(ImmutableList.of()));
        }

        ByteString bx = toProposal(requests);

        return proponent.get().propose(squadId, bx, ignoreLeader)
                .thenApply(r -> r.map(snapshot -> produceResult((TansNumberMapSnapShot) snapshot, requests)))
                .exceptionally(ex -> {
                    logger.error("at TansService.acquire", ex);
                    return ProposeResult.fail(ex.getClass().getName() + ": " + ex.getMessage());
                });
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
        private long version;

        public TansNumberMapSnapShot(PMap<String, TansNumber> numbers, long version) {
            this.numbers = numbers;
            this.version = version;
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

        synchronized void consume(long instanceId, List<KeyLong> kx) {
            if (instanceId <= this.lastInstanceId) {
                throw new IllegalStateException(String.format("dolog %d when current is %d", instanceId, this.lastInstanceId));
            }

            this.numbers = applyChange(kx, this.numbers);
            this.lastInstanceId = instanceId;
        }

        synchronized public TansNumberMapSnapShot createSnapShot() {
            return new TansNumberMapSnapShot(this.numbers, this.lastInstanceId);
        }

        private PMap<String, TansNumber> applyChange(List<KeyLong> kx, PMap<String, TansNumber> numbers0) {
            PMap<String, TansNumber> numbers1 = numbers0;
            for (KeyLong k : kx) {
                TansNumber n0 = numbers1.get(k.key());
                if (n0 == null) {
                    numbers1 = numbers1.plus(k.key(), new TansNumber(k.key(), k.value() + 1));
                }
                else {
                    numbers1 = numbers1.plus(n0.name(), n0.increase(k.value()));
                }

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
        TansMessage.TansProposal.Builder builder = TansMessage.TansProposal.newBuilder();
        for (KeyLong k : kx) {
            TansMessage.NumberProposal.Builder b = TansMessage.NumberProposal.newBuilder()
                    .setName(k.key())
                    .setValue(k.value());

            builder.addProposal(b);
        }

        return builder.build().toByteString();
    }

    private static List<KeyLong> fromProposal(ByteString message) {
        TansMessage.TansProposal proposal;
        try {
            proposal = TansMessage.TansProposal.parseFrom(message);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        ImmutableList.Builder<KeyLong> builder = ImmutableList.builder();
        for (TansMessage.NumberProposal p : proposal.getProposalList()) {
            builder.add(new KeyLong(p.getName(), p.getValue()));
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
