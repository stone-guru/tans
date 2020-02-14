package org.axesoft.jaxos.algo;

import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SquadContext {
    private static final Logger logger = LoggerFactory.getLogger(SquadContext.class);

    private JaxosSettings config;
    private int squadId;

    private volatile int proposerId = 0;
    private volatile long chosenInstanceId = 0;
    private volatile long chosenBallotId = 0;
    private volatile int chosenProposal = 0;
    private volatile long chosenTimestamp = 0;

    public SquadContext(int squadId, JaxosSettings config) {
        this.config = config;
        this.squadId = squadId;
    }

    public void recordChosenInfo(int proposerId, long chosenInstanceId, long chosenBallotId, int proposal){
        this.proposerId = proposerId;
        this.chosenInstanceId = chosenInstanceId;
        this.chosenBallotId = chosenBallotId;
        this.chosenProposal = proposal;
        this.chosenTimestamp = System.currentTimeMillis();

        //if(logger.isTraceEnabled()){
//            logger.trace("Record chosen info S{}, proposer {}, instance {}, ballotId {} at {}",
//                    this.squadId, proposerId, chosenInstanceId, chosenBallotId, new Date(chosenTimestamp));
        //}
    }

    public long chosenTimestamp(){
        return this.chosenTimestamp;
    }

    public boolean isOtherLeaderActive() {
        return proposerId > 0 && proposerId != config.serverId()
                && !isLeaderLeaseExpired(System.currentTimeMillis());
    }

    public boolean isLeader() {
        return (this.proposerId == config.serverId()) && !isLeaderLeaseExpired(System.currentTimeMillis());
    }

    public int squadId() {
        return this.squadId;
    }

    public boolean isLeaderLeaseExpired(long currentMillis) {
        return (currentMillis - this.chosenTimestamp) / 1000.0 >= config.leaderLeaseSeconds() * 1.1;
    }

    public long chosenInstanceId() {
        return this.chosenInstanceId;
    }

    public int lastProposer(){
        return this.proposerId;
    }

    public int chosenProposal(){
        return this.chosenProposal;
    }

    public Event.ChosenInfo getLastChosenInfo(){
        return new Event.ChosenInfo(chosenInstanceId, chosenBallotId, System.currentTimeMillis() - chosenTimestamp);
    }
}
