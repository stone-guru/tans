package org.axesoft.jaxos.algo;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.ConcurrentModificationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;


public class Proposer {
    private static final Logger logger = LoggerFactory.getLogger(Proposer.class);

    public enum Stage {
        NONE, PREPARING, ACCEPTING
    }

    private final JaxosSettings settings;
    private final SquadContext context;
    private final Components config;
    private final Learner learner;
    private final ProposalNumHolder proposalNumHolder;
    private final SquadMetrics metrics;

    private Timeout proposalTimeout;
    private PrepareActor prepareActor;
    private Timeout prepareTimeout;
    private AcceptActor acceptActor;
    private Timeout acceptTimeout;

    private long proposeStartTimestamp;
    private Event.BallotValue proposeValue;
    private long instanceId = 0;
    private int round = 0;
    private Stage stage;

    private BitSet allNodeIds;
    private BitSet failingNodeIds;
    private int answerNodeCount;

    private AtomicReference<CompletableFuture<ProposeResult<StateMachine.Snapshot>>> resultFutureRef;

    private Runnable proposeEndCallback;

    public Proposer(JaxosSettings settings, Components components, SquadContext context, Learner learner, SquadMetrics metrics) {
        this.settings = settings;
        this.config = components;
        this.context = context;
        this.learner = learner;
        this.proposalNumHolder = new ProposalNumHolder(this.settings.serverId(), JaxosSettings.SERVER_ID_RANGE);
        this.metrics = metrics;

        this.stage = Stage.NONE;
        this.prepareActor = new PrepareActor();
        this.acceptActor = new AcceptActor();

        this.failingNodeIds = new BitSet();
        this.allNodeIds = new BitSet();
        for (int id : this.settings.peerMap().keySet()) {
            this.allNodeIds.set(id);
        }


        this.resultFutureRef = new AtomicReference<>(null);
    }

    public void setProposeEndCallback(Runnable callback) {
        this.proposeEndCallback = callback;
    }


    public boolean propose(long instanceId, Event.BallotValue value, CompletableFuture<ProposeResult<StateMachine.Snapshot>> resultFuture) {
        synchronized (resultFutureRef) {
            if (!resultFutureRef.compareAndSet(null, resultFuture)) {
                String currentMsg = String.format("I%d %s", this.instanceId, this.proposeValue.toString());
                String requestMsg = String.format("S%d I%d %s", context.squadId(), instanceId, value.toString());
                logger.warn("S{} Previous propose not end ({}) for {}", context.squadId(), currentMsg, requestMsg);
                return false;
            }
        }

        if (!config.getCommunicator().available()) {
            endAs("Not enough server connected");
            return true;
        }

        this.instanceId = instanceId;
        this.proposeValue = value;
        this.round = 0;
        this.proposeStartTimestamp = System.nanoTime();

        if (context.isLeader()) {
            startAccept(this.proposeValue, context.chosenProposal());
        }
        else {
            int proposal0 = proposalNumHolder.proposalGreatThan(context.chosenProposal());
            startPrepare(proposal0);
        }

        this.proposalTimeout = config.getEventTimer().createTimeout(this.settings.wholeProposalTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.ProposalTimeout(this.settings.serverId(), this.context.squadId(), this.instanceId, 0));

        return true;
    }


    public boolean isRunning() {
        synchronized (resultFutureRef) {
            return resultFutureRef.get() != null;
        }
    }

    private void endAs(String error) {
        this.stage = Stage.NONE;
        if (this.proposalTimeout != null) {
            this.proposalTimeout.cancel();
            this.proposalTimeout = null;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("S{}: Propose instance({}) end with {}",
                    context.squadId(), this.instanceId, error == null ? "SUCCESS" : error);
        }
        CompletableFuture<ProposeResult<StateMachine.Snapshot>> future = this.resultFutureRef.get();
        this.resultFutureRef.set(null);

        long duration = System.nanoTime() - proposeStartTimestamp;
        if (error == null) {
            metrics.recordPropose(duration, SquadMetrics.ProposalResult.SUCCESS);
            if (!future.complete(ProposeResult.success(this.context.getStateMachineSnapshot()))) {
                logger.warn("S{} I{} proposes completed successfully but client cancelled it", this.context.squadId(), this.instanceId);
            }
        }
        else {
            if (error.startsWith("CONFLICT")) {
                metrics.recordPropose(duration, SquadMetrics.ProposalResult.CONFLICT);
            }
            else {
                metrics.recordPropose(duration, SquadMetrics.ProposalResult.OTHER);
            }
            future.complete(ProposeResult.fail(error));
        }

        if (proposeEndCallback != null) {
            this.config.getWorkerPool().queueTask(this.context.squadId(), this.proposeEndCallback);
        }
    }

    private boolean endWithMajorityCheck(int n, String step) {
        if (n <= this.settings.peerCount() / 2) {
            endAs("Not enough peers response at " + step);
            return true;
        }
        return false;
    }

    public void onProposalTimeout(Event.ProposalTimeout event) {
        if (this.stage != Stage.NONE && event.squadId() == context.squadId() && event.instanceId() == this.instanceId) {
            endAs(String.format("S%d I%d whole proposal timeout", context.squadId(), this.instanceId));
        }
        else if (logger.isDebugEnabled()) {
            logger.debug("Ignore unnecessary {}", event);
        }
    }

    private void startPrepare(int proposal0) {
        Instance i0 = this.learner.getLastChosenInstance(this.context.squadId());
        if (this.instanceId == i0.id()) {
            //other server help me finish this value
            if (i0.value().id() == this.proposeValue.id()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{} I{} Other proposer help finish when startPrepare", context.squadId(), this.instanceId);
                }
                endAs(null);
                return;
            }
        }

        if (this.instanceId != i0.id() + 1) {
            String msg = String.format("CONFLICT S%d when prepare instance %d while last chosen is %d",
                    context.squadId(), instanceId, i0.id());
            endAs(msg);
            return;
        }

        this.stage = Stage.PREPARING;
        this.round++;
        this.prepareActor.startNewRound(this.proposeValue, proposal0, i0.proposal());

        this.prepareTimeout = config.getEventTimer().createTimeout(this.settings.prepareTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.PrepareTimeout(this.settings.serverId(), this.context.squadId(), this.instanceId, this.round));
    }


    public void onPrepareReply(Event.PrepareResponse response) {
        if (logger.isTraceEnabled()) {
            logger.trace("S{} RECEIVED {}", context.squadId(), response);
        }
        //As long as a server give back a response, it's no longer a failing node
        this.failingNodeIds.clear(response.senderId());

        if (eventMatchRequest(response, Stage.PREPARING)) {
            this.prepareActor.onReply(response);
            if (this.prepareActor.isAllReplied()) {
                this.prepareTimeout.cancel();
                endPrepare();
            }
        }
        else if (logger.isDebugEnabled()) {
            logger.debug("S{} abandon unexpected response {}", context.squadId(), response);
        }
    }

    public void onPrepareTimeout(Event.PrepareTimeout event) {
        if (logger.isTraceEnabled()) {
            logger.trace("S{} {}", context.squadId(), event);
        }

        if (eventMatchRequest(event, Stage.PREPARING)) {
            this.config.getJaxosMetrics().getOrCreateSquadMetrics(context.squadId()).incPeerTimeoutCounter();
            endPrepare();
        }
        else {
            logger.warn("S{} Unnecessary {}", context.squadId(), event);
        }
    }

    private void endPrepare() {
        recordRepliedNodes(this.prepareActor.repliedNodes);

        if (endWithMajorityCheck(this.prepareActor.votedCount(), "PREPARE " + instanceId)) {
            return;
        }

        if (this.prepareActor.isApproved()) {
            //There are some other accepted value
            if (this.prepareActor.maxAcceptedProposal > 0) {
                startAccept(this.prepareActor.acceptedValue, this.prepareActor.myProposal());
            }
            //No other accepted value, use mine
            else if (this.prepareActor.maxAcceptedProposal == 0) {
                startAccept(this.proposeValue, this.prepareActor.myProposal());
            }
            else {
                throw new IllegalStateException("Should not happen, Got negative max accepted proposal");
            }
        }
        else {
            //The value of this synod has been chosen
            if (this.prepareActor.totalMaxProposal == Integer.MAX_VALUE) {
                if (this.prepareActor.acceptedBallotId == this.proposeValue.id()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("S{} I{} Other proposer help finish ", context.squadId(), this.instanceId);
                    }
                    endAs(null);
                }
                else {
                    endAs("CONFLICT " + this.instanceId + " other value chosen");
                }
            }
            else {
                sleepRandom(round, "PREPARE");
                startPrepare(proposalNumHolder.proposalGreatThan(this.prepareActor.totalMaxProposal()));
            }
        }
    }

    private void recordRepliedNodes(BitSet answerNodes) {
        if (this.answerNodeCount != answerNodes.cardinality()) {
            logger.info("S {} Answer node changed from {} to {}",
                    this.context.squadId(), this.answerNodeCount, answerNodes.cardinality());
        }

        this.answerNodeCount = answerNodes.cardinality();
        this.failingNodeIds = (BitSet) this.allNodeIds.clone();
        this.failingNodeIds.andNot(answerNodes);
    }

    private void startAccept(Event.BallotValue value, int proposal) {
        Instance i0 = this.learner.getLastChosenInstance(this.context.squadId());
        if (this.instanceId == i0.id()) {
            //other server help me finish this value
            if (i0.value().id() == this.proposeValue.id()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{} I{} Other proposer help finish when startAccept", context.squadId(), this.instanceId);
                }
                endAs(null);
                return;
            }
        }

        if (this.instanceId != i0.id() + 1) {
            String msg = String.format(" CONFLICT when accept instance %d.%d while last chosen is %d",
                    context.squadId(), instanceId, i0.id());
            endAs(msg);
            return;
        }
        this.stage = Stage.ACCEPTING;
        this.acceptActor.startAccept(value, proposal, i0.proposal());

        this.acceptTimeout = config.getEventTimer().createTimeout(this.settings.acceptTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.AcceptTimeout(settings.serverId(), this.context.squadId(), this.instanceId, this.round));
    }

    void onAcceptReply(Event.AcceptResponse response) {
        if (logger.isTraceEnabled()) {
            logger.trace("S{} RECEIVED {}", context.squadId(), response);
        }

        //As long as a server giving a response, it's no longer a failing node
        this.failingNodeIds.clear(response.senderId());

        if (eventMatchRequest(response, Stage.ACCEPTING)) {
            this.acceptActor.onReply(response);
            if (this.acceptActor.isAllReplied()) {
                this.acceptTimeout.cancel();
                endAccept();
            }
        }
    }

    void onAcceptTimeout(Event.AcceptTimeout event) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}", event);
        }
        if (eventMatchRequest(event, Stage.ACCEPTING)) {
            this.config.getJaxosMetrics().getOrCreateSquadMetrics(context.squadId()).incPeerTimeoutCounter();
            endAccept();
        }
        else {
            logger.warn("S{} Unnecessary {}", context.squadId(), event);
        }
    }

    private void endAccept() {
        if (logger.isTraceEnabled()) {
            logger.trace("S{} Process End Accept({})", context.squadId(), this.instanceId);
        }

        recordRepliedNodes(this.acceptActor.repliedNodes);

        if (endWithMajorityCheck(this.acceptActor.votedCount(), "ACCEPT")) {
            return;
        }

        if (this.acceptActor.isAccepted()) {
            this.acceptActor.notifyChosen();
            if (this.acceptActor.sentValue().id() == this.proposeValue.id()) {
                endAs(null);
            }
            else {
                String msg = String.format("CONFLICT S%d I%d send other value at Accept", context.squadId(), this.instanceId);
                endAs(msg);
            }
        }
        else if (this.acceptActor.isInstanceChosen()) {
            if (this.acceptActor.chosenBallotId == this.proposeValue.id()) {
                endAs(null);
            }
            else {
                endAs("CONFLICT Chosen by other at accept");
            }
        }
        else {
            if (this.round >= 10000) {
                endAs("CONFLICT REJECT at accept more than 100 times");
            }
            else {
                sleepRandom(round, "ACCEPT");
                startPrepare(this.proposalNumHolder.proposalGreatThan(acceptActor.maxProposal));
            }
        }
    }

    private void sleepRandom(int i, String when) {
        try {
            long t = Math.max(1L, (long) (Math.random() * this.settings.conflictSleepMillis()));
            if (logger.isDebugEnabled()) {
                logger.debug("S{} I{} conflict at {} sleep {} ms", context.squadId(), this.instanceId, when, t);
            }
            Thread.sleep(t);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private boolean eventMatchRequest(Event.BallotEvent event, Stage expectedStage) {
        if (this.stage != expectedStage) {
            logger.debug("Not at stage of PREPARING on {}", event);
            return false;
        }
        if (event.instanceId() != this.instanceId) {
            logger.debug("Instance of {} not equal to mine {}", event, this.instanceId);
            return false;
        }
        if (event.round() != this.round) {
            logger.debug("Round of {} not equal to mine {}", event, this.round);
            return false;
        }
        return true;
    }

    private class PrepareActor {
        private int proposal;
        private Event.BallotValue value;

        private int totalMaxProposal = 0;
        private int maxAcceptedProposal = 0;
        private Event.BallotValue acceptedValue;
        private long acceptedBallotId = 0;
        private final BitSet repliedNodes = new BitSet();


        private boolean someOneReject = false;
        private int approvedCount = 0;
        private long maxOtherChosenInstanceId = 0;
        private int votedCount = 0;

        public PrepareActor() {
        }

        public void startNewRound(Event.BallotValue value, int proposal, int chosenProposal) {
            //reset accumulated values
            this.totalMaxProposal = 0;
            this.maxAcceptedProposal = 0;
            this.acceptedValue = null;
            this.someOneReject = false;
            this.approvedCount = 0;
            this.maxOtherChosenInstanceId = 0;
            this.votedCount = 0;
            this.repliedNodes.clear();

            //init values for this round
            this.value = value;
            this.proposal = proposal;

            //this.valueProposer = valueProposer;

            Event.PrepareRequest req = new Event.PrepareRequest(
                    Proposer.this.settings.serverId(), Proposer.this.context.squadId(),
                    Proposer.this.instanceId, Proposer.this.round,
                    this.proposal, context.getLastChosenInfo());

            config.getCommunicator().broadcast(req);
        }

        public void onReply(Event.PrepareResponse response) {
            if (logger.isTraceEnabled()) {
                logger.trace("S{}: On PREPARE reply {}", context.squadId(), response);
            }
            if (repliedNodes.get(response.senderId())) {
                logger.warn("S{} Abandon duplicated response {}", context.squadId(), response);
                return;
            }

            repliedNodes.set(response.senderId());

            if (response.result() == Event.RESULT_STANDBY) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{}: On PrepareReply: Server {} is standby at last chosen instance id is {}",
                            context.squadId(), response.senderId(), context.chosenInstanceId());
                }
                return;
            }

            votedCount++;
            if (response.result() == Event.RESULT_SUCCESS) {
                approvedCount++;
            }
            else if (response.result() == Event.RESULT_REJECT) {
                someOneReject = true;
            }

            if (response.maxBallot() > this.totalMaxProposal) {
                this.totalMaxProposal = response.maxBallot();
            }

            if (response.acceptedBallot() > this.maxAcceptedProposal) {
                this.maxAcceptedProposal = response.acceptedBallot();
                this.acceptedValue = response.acceptedValue();
                this.acceptedBallotId = response.acceptedValue().id();
            }

            if (response.chosenInfo().instanceId() >= this.maxOtherChosenInstanceId) {
                this.maxOtherChosenInstanceId = response.chosenInfo().instanceId();
            }
        }

        private boolean isAllReplied() {
            return repliedNodes.cardinality() >= settings.peerCount() - failingNodeIds.cardinality();
        }

        private boolean isApproved() {
            return !this.someOneReject && approvedCount > settings.peerCount() / 2;
        }

        private int myProposal() {
            return this.proposal;
        }

        private int votedCount() {
            return this.votedCount;
        }

        private int totalMaxProposal() {
            return this.totalMaxProposal;
        }

        private int maxAcceptedProposal() {
            return this.maxAcceptedProposal;
        }

        private long maxOtherChosenInstanceId() {
            return this.maxOtherChosenInstanceId;
        }
    }

    private class AcceptActor {
        private Event.BallotValue sentValue;
        private int proposal;

        private int maxProposal = 0;
        private boolean someoneReject = true;
        private BitSet repliedNodes = new BitSet();
        private int acceptedCount = 0;
        private long chosenBallotId = 0;
        private int votedCount = 0;

        public void startAccept(Event.BallotValue value, int proposal, int lastChosenProposal) {
            this.proposal = proposal;
            this.sentValue = value;
            this.maxProposal = 0;
            this.someoneReject = false;
            this.repliedNodes.clear();
            this.acceptedCount = 0;
            this.chosenBallotId = 0;
            this.votedCount = 0;

            if (logger.isDebugEnabled()) {
                logger.debug("S{}: Start accept instance {} with proposal  {} ", context.squadId(), Proposer.this.instanceId, proposal);
            }

            Event.AcceptRequest request = Event.AcceptRequest.newBuilder(Proposer.this.settings.serverId(),
                    Proposer.this.context.squadId(), Proposer.this.instanceId, Proposer.this.round)
                    .setBallot(proposal)
                    .setValue(value)
                    .setChosenInfo(context.getLastChosenInfo())
                    .build();
            Proposer.this.config.getCommunicator().broadcast(request);
        }

        public void onReply(Event.AcceptResponse response) {
            this.repliedNodes.set(response.senderId());

            if (response.result() == Event.RESULT_STANDBY) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{} AcceptReply: Server {} is standby at last chosen instance id is {}",
                            context.squadId(), response.senderId(), response.chosenInfo().instanceId());
                }
                return;
            }

            votedCount++;
            if (response.result() == Event.RESULT_REJECT) {
                this.someoneReject = true;
            }
            else if (response.result() == Event.RESULT_SUCCESS) {
                this.acceptedCount++;
            }

            if (response.maxBallot() == Integer.MAX_VALUE) {
                this.chosenBallotId = response.acceptedBallotId();
            }

            if (response.maxBallot() > this.maxProposal) {
                this.maxProposal = response.maxBallot();
            }
        }

        boolean isAccepted() {
            return !this.someoneReject && settings.reachQuorum(acceptedCount);
        }

        boolean isAllReplied() {
            return this.repliedNodes.cardinality() >= settings.peerCount() - failingNodeIds.cardinality();
        }

        boolean isInstanceChosen() {
            return this.chosenBallotId > 0;
        }

        int votedCount() {
            return this.votedCount;
        }

        Event.BallotValue sentValue() {
            return this.sentValue;
        }

        void notifyChosen() {
            if (logger.isDebugEnabled()) {
                logger.debug("S{} Notify instance {} chosen", context.squadId(), Proposer.this.instanceId);
            }

            //Then notify other peers
            Event notify = new Event.ChosenNotify(Proposer.this.settings.serverId(), Proposer.this.context.squadId(),
                    Proposer.this.instanceId, this.proposal, this.sentValue.id());
            Proposer.this.config.getCommunicator().selfFirstBroadcast(notify);
        }
    }


    private static class ProposalNumHolder {
        private int serverId;
        private int range;

        public ProposalNumHolder(int serverId, int range) {
            checkArgument(serverId > 0 && serverId < range, "server id %d beyond range");
            this.serverId = serverId;
            this.range = range;
        }

        public int getProposal0() {
            return this.serverId;
        }

        public int proposalGreatThan(int proposal) {
            final int r = this.range;
            return ((proposal / r) + 1) * r + this.serverId;
        }
    }
}
