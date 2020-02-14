package org.axesoft.jaxos.algo;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosSettings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A Squad is a composition of Proposer and Acceptor, and works as a individual paxos server.
 *
 * @sine 2019/8/25.
 */
public class Squad implements EventDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(Squad.class);

    private Acceptor acceptor;
    private Proposer proposer;
    private SquadContext context;
    private SquadMetrics metrics;
    private JaxosSettings settings;
    private StateMachineRunner stateMachineRunner;
    private Components components;

    private Timeout learnTimeout;
    private long timestampOfLearnReq;

    public Squad(int squadId, JaxosSettings settings, Components components, StateMachine machine) {
        this.settings = settings;
        this.components = components;
        this.context = new SquadContext(squadId, this.settings);
        this.metrics = components.getJaxosMetrics().getOrCreateSquadMetrics(squadId);
        this.stateMachineRunner = new StateMachineRunner(squadId, machine);
        this.proposer = new Proposer(this.settings, components, this.context, (Learner) stateMachineRunner);
        this.acceptor = new Acceptor(this.settings, components, this.context, (Learner) stateMachineRunner);

        //indicate that there is no lear request sent
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

    /**
     * @param v value to be proposed
     * @throws InterruptedException
     */
    public ListenableFuture<Void> propose(long instanceId, Event.BallotValue v, boolean ignoreLeader, SettableFuture<Void> resultFuture) {
        attachMetricsListener(resultFuture);

        if (this.context.isOtherLeaderActive() && this.settings.leaderOnly() && !ignoreLeader) {
            if (logger.isDebugEnabled()) {
                logger.debug("S{} I{} redirect to {}", context.squadId(), instanceId, this.context.lastProposer());
            }
            resultFuture.setException(new RedirectException(this.context.lastProposer()));
        }
        else {
            proposer.propose(instanceId, v, resultFuture);
        }

        return resultFuture;
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

    private void attachMetricsListener(ListenableFuture<Void> future) {
        final long startNano = System.nanoTime();

        Futures.addCallback(future, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable Void result) {
                record(SquadMetrics.ProposalResult.SUCCESS);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof ProposalConflictException) {
                    record(SquadMetrics.ProposalResult.CONFLICT);
                }
                else {
                    record(SquadMetrics.ProposalResult.OTHER);
                }
            }

            private void record(SquadMetrics.ProposalResult result) {
                Squad.this.metrics.recordPropose(System.nanoTime() - startNano, result);
            }

        }, MoreExecutors.directExecutor());
    }

    public long lastChosenInstanceId() {
        return this.context.chosenInstanceId();
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
            case ACCEPTED_NOTIFY: {
                acceptor.onChosenNotify(((Event.ChosenNotify) event));
                this.metrics.recordLeader(event.senderId());
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
            case CHOSEN_QUERY_RESPONSE: {
                long otherLast = ((Event.ChosenQueryResponse) event).chosenInstanceIdOf(context.squadId());
                long last = this.lastChosenInstanceId();
                if (last < otherLast - 1 && learnTimeout == null) {
                    startLearn(event.senderId(), last, otherLast);
                }
                return null;
            }
            case LEARN_REQUEST: {
                if(event.senderId() == this.settings.serverId()){
                    logger.warn("S{} got learn request {} from self", this.context.squadId(), event);
                } else {
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
                startLearn(receivedEvent.senderId(), this.context.chosenInstanceId(), chosenInfo.instanceId());
            }
        }
    }

    private void startLearn(int senderId, long myLast, long otherLast) {
        this.components.getWorkerPool().queueTask(context().squadId(), () -> {
            Event.Learn learn = new Event.Learn(settings.serverId(), context.squadId(), myLast + 1, otherLast);
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
                context.squadId(), response.checkPoint().instanceId(),
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
        Instance last = checkPoint.lastInstance();
        Instance expectedLast = this.components.getLogger().loadLastInstance(context.squadId());

        //When last is empty(id is 0), this loop do nothing
        List<Instance> ix = new ArrayList<>();
        for (long i = checkPoint.instanceId() + 1; i <= expectedLast.id(); i++) {
            Instance instance = this.components.getLogger().loadInstance(context.squadId(), i);
            //It happens rarely
            if (instance.isEmpty()) {
                logger.warn("S{} Instance {} not found in DB, with checkPoint({}) last({}) ", context.squadId(), i, checkPoint.instanceId(), expectedLast.id());
                break;
            }
            last = instance;
            ix.add(instance);
        }

        this.stateMachineRunner.restoreFromCheckPoint(checkPoint, ix);
        this.context.recordChosenInfo(0, last.id(), last.value().id(), last.proposal());

        logger.info("S{} restored to instance {}", context.squadId(), last.id());
    }
}
