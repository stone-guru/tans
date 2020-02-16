package org.axesoft.jaxos;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

public class JaxosSettings {
    public static final int SERVER_ID_RANGE = 100;

    public static class Peer {
        private int id;
        private String address;
        private int port;

        public Peer(int id, String address, int port) {
            this.id = id;
            this.address = address;
            this.port = port;
        }

        public int id() {
            return id;
        }

        public String address() {
            return address;
        }

        public int port() {
            return port;
        }

        @Override
        public String toString() {
            return "Peer{" +
                    "id=" + id +
                    ", address='" + address + '\'' +
                    ", port=" + port +
                    '}';
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int serverId;
        private boolean leaderOnly = true;
        private String dbDirectory;
        private long wholeProposalTimeoutMillis = 1500;
        private long prepareTimeoutMillis = 150;
        private long acceptTimeoutMillis =  150;
        private int partitionNumber = 1;
        private int checkPointMinutes = 1;
        private Duration syncInterval = Duration.ofMillis(1000);
        private long conflictSleepMillis = 50;
        private int leaderLeaseSeconds = 3;
        private int algoThreadNumber = 3;
        private Duration learnTimeout = Duration.ofMillis(1500);
        private long learnInstanceLimit = 50000;
        private long sendInstanceLimit = 20000;
        private String loggerImplementation = "rocksdb";

        private Function<ByteString, String> valueVerboser;

        private ImmutableMap.Builder<Integer, Peer> peerBuilder = ImmutableMap.<Integer, Peer>builder();

        public Builder setServerId(int serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder addPeer(Peer peer){
            peerBuilder.put(peer.id(), peer);
            return this;
        }

        public Builder setLeaderOnly(boolean leaderOnly) {
            this.leaderOnly = leaderOnly;
            return this;
        }

        public Builder setDbDirectory(String dbDirectory) {
            this.dbDirectory = dbDirectory;
            return this;
        }

        public Builder setWholeProposalTimeoutMillis(long wholeProposalTimeoutMillis) {
            this.wholeProposalTimeoutMillis = wholeProposalTimeoutMillis;
            return this;
        }

        public Builder setPrepareTimeoutMillis(long prepareTimeoutMillis) {
            this.prepareTimeoutMillis = prepareTimeoutMillis;
            return this;
        }

        public Builder setAcceptTimeoutMillis(long acceptTimeoutMillis) {
            this.acceptTimeoutMillis = acceptTimeoutMillis;
            return this;
        }

        public Builder setValueVerboser(Function<ByteString, String> valueVerboser) {
            this.valueVerboser = valueVerboser;
            return this;
        }

        public Builder setPartitionNumber(int partitionNumber) {
            this.partitionNumber = partitionNumber;
            return this;
        }

        public Builder setCheckPointMinutes(int checkPointMinutes) {
            this.checkPointMinutes = checkPointMinutes;
            return this;
        }

        public Builder setSyncInterval(Duration syncInterval) {
            this.syncInterval = checkNotNull(syncInterval, "param syncInterval is null");
            return this;
        }

        public Builder setConflictSleepMillis(long conflictSleepMillis) {
            this.conflictSleepMillis = conflictSleepMillis;
            return this;
        }

        public Builder setLeaderLeaseSeconds(int leaderLeaseSeconds) {
            this.leaderLeaseSeconds = leaderLeaseSeconds;
            return this;
        }

        public Builder setAlgoThreadNumber(int algoThreadNumber) {
            this.algoThreadNumber = algoThreadNumber;
            return this;
        }

        public Builder setLearnTimeout(Duration learnTimeout) {
            this.learnTimeout = checkNotNull(learnTimeout, "Param learnTimeout is null");
            return this;
        }

        public Builder setLearnInstanceLimit(long learnInstanceLimit) {
            this.learnInstanceLimit = learnInstanceLimit;
            return this;
        }

        public Builder setSendInstanceLimit(long sendInstanceLimit) {
            this.sendInstanceLimit = sendInstanceLimit;
            return this;
        }

        public Builder setLoggerImplementation(String loggerImplementation) {
            this.loggerImplementation = loggerImplementation;
            return this;
        }

        public JaxosSettings build(){
            JaxosSettings config = new JaxosSettings();
            config.serverId = this.serverId;
            config.peerMap = this.peerBuilder.build();
            config.leaderOnly = this.leaderOnly;
            config.leaderLeaseSeconds = this.leaderLeaseSeconds;
            config.dbDirectory = this.dbDirectory;
            config.valueVerboser = this.valueVerboser;
            config.wholeProposalTimeoutMillis = this.wholeProposalTimeoutMillis;
            config.prepareTimeoutMillis = this.prepareTimeoutMillis;
            config.acceptTimeoutMillis = this.acceptTimeoutMillis;
            config.partitionNumber = this.partitionNumber;
            config.checkPointMinutes = this.checkPointMinutes;
            config.syncInterval = this.syncInterval;
            config.conflictSleepMillis = this.conflictSleepMillis;
            config.algoThreadNumber = this.algoThreadNumber;
            config.learnTimeout = this.learnTimeout;
            config.learnInstanceLimit = this.learnInstanceLimit;
            config.sendInstanceLimit = this.sendInstanceLimit;
            config.loggerImplementation = this.loggerImplementation;
            return config;
        }
    }

    private Map<Integer, Peer> peerMap;
    private int serverId;
    private int leaderLeaseSeconds;
    private boolean leaderOnly;
    private String dbDirectory;
    private long wholeProposalTimeoutMillis;
    private long prepareTimeoutMillis;
    private long acceptTimeoutMillis;
    private int partitionNumber;
    private int checkPointMinutes;
    private Duration syncInterval;
    private long conflictSleepMillis;
    private int algoThreadNumber;
    private Duration learnTimeout;
    private long learnInstanceLimit = 50000;
    private long sendInstanceLimit = 20000;
    private Function<ByteString, String> valueVerboser;
    private String loggerImplementation;

    private JaxosSettings() {
    }

    public Peer getPeer(int id){
        return peerMap.get(id);
    }

    public Map<Integer, Peer> peerMap() {
        return this.peerMap;
    }

    public int serverId() {
        return this.serverId;
    }

    public Peer self() {
        return this.peerMap.get(this.serverId);
    }

    public int peerCount() {
        return this.peerMap.size();
    }

    public int leaderLeaseSeconds(){
        return this.leaderLeaseSeconds;
    }

    public boolean leaderOnly(){
        return this.leaderOnly;
    }

    public String dbDirectory(){
        return this.dbDirectory;
    }

    public long wholeProposalTimeoutMillis(){
        return this.wholeProposalTimeoutMillis;
    }

    public long prepareTimeoutMillis(){
        return this.prepareTimeoutMillis;
    }

    public long acceptTimeoutMillis(){
        return this.acceptTimeoutMillis;
    }

    public boolean reachQuorum(int n){
        return n > this.peerMap.size() / 2;
    }

    public int partitionNumber(){
        return this.partitionNumber;
    }

    public int checkPointMinutes(){
        return this.checkPointMinutes;
    }

    public Function<ByteString, String> valueVerboser(){
        return this.valueVerboser;
    }

    public Duration syncInterval (){
        return this.syncInterval;
    }

    public long conflictSleepMillis() {
        return this.conflictSleepMillis;
    }

    public int algoThreadNumber() {
        return this.algoThreadNumber;
    }

    public Duration learnTimeout(){
        return this.learnTimeout;
    }

    public long learnInstanceLimit(){
        return this.learnInstanceLimit;
    }

    public long sendInstanceLimit(){
        return this.sendInstanceLimit;
    }

    public String loggerImplementation(){
        return this.loggerImplementation;
    }

    @Override
    public String toString() {
        return "JaxosSettings{" +
                "peerMap=" + peerMap +
                ", serverId=" + serverId +
                ", dbDirectory='" + dbDirectory + '\'' +
                ", leaderLeaseSeconds=" + leaderLeaseSeconds +
                ", leaderOnly=" + leaderOnly +
                ", wholeProposalTimeoutMillis=" + wholeProposalTimeoutMillis +
                ", prepareTimeoutMillis=" + prepareTimeoutMillis +
                ", acceptTimeoutMillis=" + acceptTimeoutMillis +
                ", partitionNumber=" + partitionNumber +
                ", algoThreadNumber=" + algoThreadNumber +
                ", conflictSleepMillis=" + conflictSleepMillis +
                ", checkPointMinutes=" + checkPointMinutes +
                ", loggerSyncInterval=" + syncInterval +
                ", learnInstanceLimit=" + learnInstanceLimit +
                ", sendInstanceLimit=" + sendInstanceLimit +
                ", loggerImplementation=" + loggerImplementation +
                '}';
    }
}
