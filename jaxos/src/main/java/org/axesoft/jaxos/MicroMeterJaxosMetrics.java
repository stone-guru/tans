package org.axesoft.jaxos;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.axesoft.jaxos.algo.JaxosMetrics;
import org.axesoft.jaxos.algo.SquadMetrics;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gaoyuan
 * @sine 2019/9/7.
 */
public class MicroMeterJaxosMetrics implements JaxosMetrics {
    private final int serverId;
    private final PrometheusMeterRegistry registry;
    private Timer loggerSyncTimer;
    private Timer loggerSaveTimer;
    private Timer loggerLoadTimer;
    private Timer loggerDeleteTimer;
    private Timer loggerCheckPointTimer;
    private Gauge restoreTimeGauge;
    private double restoreTimeSeconds;
    private Map<Integer, SquadMetrics> squadMetricsMap;

    public MicroMeterJaxosMetrics(int serverId) {
        this.serverId = serverId;
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.registry.config().commonTags("server", Integer.toString(this.serverId));

        this.squadMetricsMap = new ConcurrentHashMap<>();

        this.initLoggerMetrics();
    }

    @Override
    public String format() {
        return registry.scrape();
    }

    @Override
    public SquadMetrics getOrCreateSquadMetrics(int squadId) {
        return this.squadMetricsMap.computeIfAbsent(squadId, k -> new MicroMeterSquadMetrics(k, this.registry));
    }

    @Override
    public void recordRestoreElapsedMillis(long millis) {
        this.restoreTimeSeconds = millis/1000.0;
    }

    @Override
    public void recordLoggerLoadElapsed(long nanos) {
        this.loggerLoadTimer.record(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordLoggerSaveElapsed(long nanos) {
        this.loggerSaveTimer.record(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordLoggerSyncElapsed(long nanos) {
        this.loggerSyncTimer.record(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordLoggerDeleteElapsed(long nanos) {
        this.loggerDeleteTimer.record(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordLoggerSaveCheckPointElapse(long nanos) {
        this.loggerCheckPointTimer.record(nanos, TimeUnit.NANOSECONDS);
    }

    private static Timer.Builder setGlobalTimerConfigs(Timer.Builder builder) {
        return builder.publishPercentiles(0.5, 0.85, 0.99, 0.99)
                .sla(Duration.ofMillis(3), Duration.ofMillis(10), Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofSeconds(1))
                .distributionStatisticExpiry(Duration.ofSeconds(5))
                .distributionStatisticBufferLength(3);
    }

    private void initLoggerMetrics() {
        this.loggerLoadTimer = setGlobalTimerConfigs(Timer.builder("logger.load.duration")
                .description("The time for each propose"))
                .register(registry);

        this.loggerSaveTimer = setGlobalTimerConfigs(Timer.builder("logger.save.duration")
                .description("The time for each propose"))
                .register(registry);

        this.loggerSyncTimer = setGlobalTimerConfigs(Timer.builder("logger.sync.duration")
                .description("The time for each propose"))
                .register(registry);

        this.loggerDeleteTimer = setGlobalTimerConfigs(Timer.builder("logger.delete.duration")
                .description("The time for each propose"))
                .register(registry);

        this.loggerCheckPointTimer = setGlobalTimerConfigs(Timer.builder("logger.checkPoint.duration")
                .description("The time for saving checkpoint"))
                .distributionStatisticExpiry(Duration.ofMillis(10))
                .distributionStatisticBufferLength(3)
                .register(registry);

        this.restoreTimeGauge = Gauge.builder("tans.restore.seconds", () -> this.restoreTimeSeconds)
                .description("The seconds of restoring whole state from database")
                .register(registry);
    }

    private static class MicroMeterSquadMetrics implements SquadMetrics {
        private final int squadId;

        private Counter proposeCounter;
        private Counter successCounter;
        private Counter conflictCounter;
        private Counter otherCounter;
        private Counter peerTimeoutCounter;
        private Timer proposeTimer;
        private Timer acceptTimer;
        private Timer learnTimer;
        private Timer teachTimer;
        private AtomicInteger leaderId;

        public MicroMeterSquadMetrics(int squadId, PrometheusMeterRegistry registry) {
            this.squadId = squadId;

            this.proposeCounter = Counter.builder("propose.total")
                    .description("The total times of propose request")
                    .tags("squad", Integer.toString(this.squadId))
                    .register(registry);

            this.successCounter = Counter.builder("propose.success")
                    .description("The success times of propose request")
                    .tags("squad", Integer.toString(this.squadId))
                    .register(registry);

            this.conflictCounter = Counter.builder("propose.conflict")
                    .description("The conflict times of propose request")
                    .tags("squad", Integer.toString(this.squadId))
                    .register(registry);

            this.otherCounter = Counter.builder("propose.other")
                    .description("The times of propose other result")
                    .tags("squad", Integer.toString(this.squadId))
                    .register(registry);

            this.peerTimeoutCounter = Counter.builder("peer.timeout")
                    .description("The times of propose timeout")
                    .tags("squad", Integer.toString(this.squadId))
                    .register(registry);

            this.proposeTimer = setGlobalTimerConfigs(Timer.builder("propose.duration")
                    .description("The time for each propose")
                    .tags("squad", Integer.toString(this.squadId)))
                    .register(registry);


            this.acceptTimer = setGlobalTimerConfigs(Timer.builder("accept.duration")
                    .description("The time for each accept")
                    .tags("squad", Integer.toString(this.squadId)))
                    .register(registry);

            this.learnTimer = setGlobalTimerConfigs(Timer.builder("learn.duration")
                    .description("The time between sent learn request and processed response")
                    .tags("squad", Integer.toString(this.squadId)))
                    .register(registry);

            this.teachTimer = setGlobalTimerConfigs(Timer.builder("teach.duration")
                    .description("The duration for prepare a learn response")
                    .tags("squad", Integer.toString(this.squadId)))
                    .register(registry);

            this.leaderId = new AtomicInteger(0);
            Gauge.builder("squad.leader", this.leaderId::get)
                    .description("The leader id of this squad")
                    .tags("squad", Integer.toString(this.squadId))
                    .register(registry);
        }


        public void recordAccept(long nanos) {
            acceptTimer.record(nanos, TimeUnit.NANOSECONDS);
        }

        public void recordPropose(long nanos, SquadMetrics.ProposalResult result) {
            proposeCounter.increment();
            proposeTimer.record(nanos, TimeUnit.NANOSECONDS);
            switch (result) {
                case SUCCESS:
                    successCounter.increment();
                    break;
                case CONFLICT:
                    conflictCounter.increment();
                    break;
                default:
                    otherCounter.increment();
            }
        }

        @Override
        public void recordLearnMillis(long millis) {
            this.learnTimer.record(millis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void recordTeachNanos(long nanos) {
            this.teachTimer.record(nanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public void recordLeader(int serverId) {
            if (this.leaderId.get() != serverId) {
                this.leaderId.set(serverId);
            }
        }

        @Override
        public void incPeerTimeoutCounter() {
            this.peerTimeoutCounter.increment();
        }
    }
}
