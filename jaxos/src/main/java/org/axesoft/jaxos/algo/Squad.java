package org.axesoft.jaxos.algo;

import com.google.common.collect.ImmutableList;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Squad is a composition of Proposer and Acceptor, and works as a individual paxos server.
 *
 * @sine 2019/8/25.
 */
public class Squad implements EventDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(Squad.class);

    private static class ProposeRequest {
        Event.BallotValue value;
        boolean ignoreLeader;
        CompletableFuture<ProposeResult<StateMachine.Snapshot>> resultFuture;

        public ProposeRequest(Event.BallotValue value, boolean ignoreLeader, CompletableFuture<ProposeResult<StateMachine.Snapshot>> resultFuture) {
            this.value = value;
            this.ignoreLeader = ignoreLeader;
            this.resultFuture = resultFuture;
        }
    }

    private Acceptor acceptor;
    private Proposer proposer;
    private SquadContext context;
    private SquadMetrics metrics;
    private JaxosSettings settings;
    private StateMachineRunner stateMachineRunner;
    private Components components;

    private Timeout learnTimeout;
    private long timestampOfLearnReq;

    private volatile Instance preLifeLastInstance;

    private Deque<ProposeRequest> proposeRequestQueue = new ConcurrentLinkedDeque<>();
    private AtomicInteger proposeRequestQueueSize = new AtomicInteger(0);

    public Squad(int squadId, JaxosSettings settings, Components components, StateMachine machine) {
        this.settings = settings;
        this.components = components;
        this.context = new SquadContext(squadId, this.settings, machine);

        this.metrics = components.getJaxosMetrics().getOrCreateSquadMetrics(squadId);
        this.metrics.createLeaderGaugeIfNotSet(this.context::leaderId);
        this.metrics.createInstanceIdGaugeIfNotSet(this.context::chosenInstanceId);
        this.metrics.createProposeQueueSizeIfNotSet(this.proposeRequestQueueSize::get);

        this.stateMachineRunner = new StateMachineRunner(squadId, machine);
        this.proposer = new Proposer(this.settings, components, this.context, (Learner) stateMachineRunner, this.metrics);
        this.proposer.setProposeEndCallback(this::fireNextPropose);
        this.acceptor = new Acceptor(this.settings, components, this.context, (Learner) stateMachineRunner);

        //indicate that there is no learn request sent
        this.learnTimeout = null;
    }

    /**
     * The id of this squad
     */
    public int id() {
        return this.context.squadId();
    }

    /**
     * The context of this squad
     *
     * @return not null
     */
    public SquadContext context() {
        return this.context;
    }

    public Instance prelifeLastInstance() {
        return this.preLifeLastInstance;
    }

    public boolean isPrelifeLastConfirmed() {
        return this.preLifeLastInstance.id() <= this.context.chosenInstanceId();
    }

    /**
     * @param v value to be proposed
     * @throws InterruptedException
     */
    public void propose(Event.BallotValue v, boolean ignoreLeader, CompletableFuture<ProposeResult<StateMachine.Snapshot>> resultFuture) {
        this.metrics.incProposeRequestCounter();
        ProposeRequest request = new ProposeRequest(v, ignoreLeader, resultFuture);
        if (proposeRequestQueueSize.get() >= 50 * 1024) {
            resultFuture.completeExceptionally(new RuntimeException("Waiting Queue full"));
        }
        else {
            proposeRequestQueue.addLast(request);
            proposeRequestQueueSize.incrementAndGet();

            if (!proposer.isRunning()) {
                this.fireNextPropose();
            }
        }
    }

    private void fireNextPropose() {
        components.getWorkerPool().queueTask(this.context.squadId(), () -> {
            while (!proposer.isRunning()) {
                ProposeRequest request = proposeRequestQueue.poll();
                if (request == null) {
                    break;
                }
                proposeRequestQueueSize.decrementAndGet();

                //client can cancel the propose request due to timeout or other reason.
                if (request.resultFuture.isDone()) {
                    continue;
                }
                if (this.context.isOtherLeaderActive() && this.settings.leaderOnly() && !request.ignoreLeader) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("S{} redirect to {}", context.squadId(), this.context.lastProposer());
                    }
                    request.resultFuture.complete(ProposeResult.redirectTo(this.context.lastProposer()));
                }
                else {
                    boolean accepted = proposer.propose(this.context.chosenInstanceId() + 1, request.value, request.resultFuture);
                    //there is only one another place may cause propose not accepted is "directPropose" ,
                    // which is for confirm the last instance after restart
                    if (!accepted) {
                        proposeRequestQueue.addFirst(request);
                        proposeRequestQueueSize.incrementAndGet();
                    }
                }
            }
        });
    }

    public void directPropose(long instanceId, Event.BallotValue value, CompletableFuture<ProposeResult<StateMachine.Snapshot>> resultFuture) {
        components.getWorkerPool().queueTask(context.squadId(), () -> {
            if (!proposer.propose(instanceId, value, resultFuture)) {
                resultFuture.complete(ProposeResult.fail("another propose is running"));
            }
        });
    }

    @Override
    public Event processEvent(Event request) {
        if (request instanceof Event.BallotEvent) {
            Event.BallotEvent result = processBallotEvent((Event.BallotEvent) request);
            examChosenLag((Event.BallotEvent) request);
            return result;
        }
        else if (request instanceof Event.InstanceEvent) {
            return processLearnerEvent(request);
        }
        else {
            throw new UnsupportedOperationException("Unknown event type of " + request.code());
        }
    }

    private Event.BallotEvent processBallotEvent(Event.BallotEvent event) {
        switch (event.code()) {
            case PREPARE: {
                return acceptor.prepare((Event.PrepareRequest) event);
            }
            case PREPARE_RESPONSE: {
                proposer.onPrepareReply((Event.PrepareResponse) event);
                return null;
            }
            case PREPARE_TIMEOUT: {
                proposer.onPrepareTimeout((Event.PrepareTimeout) event);
                return null;
            }
            case ACCEPT: {
                long nano = System.nanoTime();
                Event.BallotEvent e = acceptor.accept((Event.AcceptRequest) event);
                this.metrics.recordAccept(System.nanoTime() - nano);
                return e;
            }
            case ACCEPT_RESPONSE: {
                proposer.onAcceptReply((Event.AcceptResponse) event);
                return null;
            }
            case ACCEPT_TIMEOUT: {
                proposer.onAcceptTimeout((Event.AcceptTimeout) event);
                return null;
            }
            case CHOSEN_NOTIFY: {
                acceptor.onChosenNotify(((Event.ChosenNotify) event));
                return null;
            }
            case PROPOSAL_TIMEOUT: {
                proposer.onProposalTimeout((Event.ProposalTimeout) event);
                return null;
            }
            default: {
                throw new UnsupportedOperationException(event.code().toString());
            }
        }
    }

    private Event processLearnerEvent(Event event) {
        switch (event.code()) {
            case LEARN_REQUEST: {
                if (event.senderId() == this.settings.serverId()) {
                    logger.warn("S{} got learn request {} from self", this.context.squadId(), event);
                }
                else {
                    this.components.getWorkerPool().submitBackendTask(() -> this.onLearnRequest((Event.Learn) event));
                }
                return null;
            }
            case LEARN_RESPONSE: {
                this.onLearnResponse((Event.LearnResponse) event);
                return null;
            }
            case LEARN_TIMEOUT: {
                if (this.learnTimeout != null) {
                    logger.warn("S{} learn timeout", context.squadId());
                    this.learnTimeout = null;
                }
                return null;
            }
            default: {
                throw new UnsupportedOperationException(event.code().toString());
            }
        }
    }

    private void examChosenLag(Event.BallotEvent receivedEvent) {
        Event.ChosenInfo chosenInfo = receivedEvent.chosenInfo();
        if (chosenInfo != null && this.learnTimeout == null && !this.acceptor.isFaulty()) {
            if ((this.context.chosenInstanceId() + 1 < chosenInfo.instanceId()) ||
                    (this.context.chosenInstanceId() < chosenInfo.instanceId()
                            && chosenInfo.elapsedMillis() >= 1000)) {
                startLearn(receivedEvent.senderId(), this.context.chosenInstanceId());
            }
        }
    }

    private void startLearn(int senderId, long myLast) {
        this.components.getWorkerPool().queueTask(context().squadId(), () -> {
            Event.Learn learn = new Event.Learn(settings.serverId(), context.squadId(), myLast + 1);
            this.components.getCommunicator().send(learn, senderId);
            logger.info("S{} Sent learn request {} to server {}", context.squadId(), learn, senderId);
        });

        this.learnTimeout = this.components.getEventTimer().createTimeout(settings.learnTimeout().toMillis(), TimeUnit.MILLISECONDS,
                new Event.LearnTimeout(settings.serverId(), context.squadId()));
        this.timestampOfLearnReq = System.currentTimeMillis();
    }


    private void onLearnRequest(Event.Learn request) {
        long n0 = System.nanoTime();
        Event result = null;
        long high = this.context.chosenInstanceId();
        long requiredInstanceCount = high - request.lowInstanceId() + 1;
        Optional<List<Instance>> ix0 = requiredInstanceCount <= this.settings.learnInstanceLimit() ?
                loadInstances(request.lowInstanceId(), Long.min(high, request.lowInstanceId() + this.settings.sendInstanceLimit()))
                : Optional.empty();

        if (ix0.isEmpty()) {
            CheckPoint checkPoint = this.stateMachineRunner.makeCheckPoint();
            result = new Event.LearnResponse(settings.serverId(), context.squadId(), Collections.emptyList(), checkPoint);
        }
        else {
            Event.LearnResponse resp = new Event.LearnResponse(settings.serverId(), context.squadId(), ix0.get(), CheckPoint.EMPTY);
            logger.info("S{} prepared learn response from {} to {} for server {}", context.squadId(),
                    resp.lowInstanceId(), resp.highInstanceId(), request.senderId());
            result = resp;
        }

        this.components.getCommunicator().send(result, request.senderId());

        this.metrics.recordTeachNanos(System.nanoTime() - n0);
    }

    private Optional<List<Instance>> loadInstances(long low, long high) {
        ImmutableList.Builder<Instance> builder = ImmutableList.builder();

        for (long id = low; id <= high; id++) {
            Instance p = this.components.getLogger().loadInstance(context.squadId(), id);
            if (p.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{} Making learn response, Server {} lack instance {} ", context.squadId(), settings.serverId(), id);
                }
                return Optional.empty();
            }

            builder.add(p);
        }

        return Optional.of(builder.build());
    }

    private void onLearnResponse(Event.LearnResponse response) {
        logger.info("S{} learn CheckPoint {} with {} instances from {} to {}",
                context.squadId(), response.checkPoint().lastInstance().id(),
                response.highInstanceId() == 0 ? 0 : response.highInstanceId() - response.lowInstanceId() + 1,
                response.lowInstanceId(), response.highInstanceId());

        //sometimes the learn response will come back lately
        if (this.learnTimeout != null) {
            this.learnTimeout.cancel();
            this.learnTimeout = null;
        }

        List<Instance> ix = response.instances();
        CheckPoint checkPoint = response.checkPoint();

        if (!checkPoint.isEmpty()) {
            saveCheckPoint(checkPoint, false);
        }

        for (Instance i : ix) {
            //System.out.println("on lean response save " + i.toString());
            this.components.getLogger().saveInstance(i.squadId(), i.id(), i.proposal(), i.value());
        }

        this.stateMachineRunner.restoreFromCheckPoint(checkPoint, ix);

        Instance last = ix.isEmpty() ? checkPoint.lastInstance() : ix.get(ix.size() - 1);
        //FIXME use learn response sender as leader works fine?
        this.context.recordChosenInfo(response.senderId(), last.id(), last.value().id(), last.proposal());

        this.metrics.recordLearnMillis(System.currentTimeMillis() - this.timestampOfLearnReq);
    }

    public void saveCheckPoint() {
        CheckPoint checkPoint = this.stateMachineRunner.makeCheckPoint();
        saveCheckPoint(checkPoint, true);
    }

    public void saveCheckPoint(CheckPoint checkPoint, boolean deleteOldInstances) {
        this.components.getLogger().saveCheckPoint(checkPoint, deleteOldInstances);
        logger.info("S{} Saved {} ", checkPoint.squadId(), checkPoint);
    }

    public void restoreFromDB() {
        CheckPoint checkPoint = this.components.getLogger().loadLastCheckPoint(context.squadId());
        this.stateMachineRunner.restoreFromCheckPoint(checkPoint, Collections.emptyList());

        Instance lastInstance = this.components.getLogger().loadInstance(context.squadId(), checkPoint.lastInstance().id() + 1);
        Instance appliedInstance = checkPoint.lastInstance();
        if (!lastInstance.isEmpty()) {
            Instance nextInstance;
            do {
                nextInstance = this.components.getLogger().loadInstance(context.squadId(), lastInstance.id() + 1);
                if (!nextInstance.isEmpty()) {
                    this.stateMachineRunner.learnValue(lastInstance);
                    appliedInstance = lastInstance;
                    lastInstance = nextInstance;
                }
            } while (!nextInstance.isEmpty());
        }

        //For lastInstance
        //The stored last instance may be an accepted value or just a prepare message.
        //If it's an accepted value, we don't know whether it has been chosen or not. it should be confirmed
        //If it's just a prepare message, no need to confirm it again and only let acceptor restore the state from it
        this.preLifeLastInstance = lastInstance.value().isEmpty() ? appliedInstance : lastInstance;

        this.context.recordChosenInfo(0, appliedInstance.id(), appliedInstance.value().id(), appliedInstance.proposal());
        this.acceptor.restore(lastInstance);

        String confirmMessage = lastInstance.isEmpty() || lastInstance.value().isEmpty() ? "NONE" : lastInstance.toString();
        logger.info("S{} restored to instance {}, to be confirmed {}", context.squadId(), appliedInstance.id(), confirmMessage);
    }

    public void consume(Instance i) {
        this.stateMachineRunner.learnValue(i);
    }
}
