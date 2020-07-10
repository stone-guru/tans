package org.axesoft.jaxos.algo;

import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;


public class Acceptor {
    private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);

    /**
     * Shared global settings
     */
    private final JaxosSettings settings;
    /**
     * The shared dependent components
     */
    private final Components components;
    /**
     * The shared context of the bellowed squad
     */
    private final SquadContext context;
    /**
     * Learner for each accepted proposal
     */
    private final Learner learner;
    /**
     * Indicate that this acceptor has encountered an unrecoverable error and can not work any more
     */
    private boolean faulty;


    /**
     * The instanceId from most recent accepted PREPARE or ACCEPT message
     */
    private long currentInstanceId;
    /**
     * The maximal ballot of this round, and initial value of which is 0
     */
    private int maxBallot;
    /**
     * The ballot of the accepted value from an ACCEPT message. It will change after accepting a new value.
     */
    private int acceptedBallot;
    /**
     * The accepted value from an ACCEPT message.
     */
    private Event.BallotValue acceptedValue;

    /**
     * constructor
     *
     * @param settings   global settings, not null
     * @param components the dependent global components, not null
     * @param context    the shared context of the squad, not null
     * @param learner    the leaner, not null
     */
    public Acceptor(JaxosSettings settings, Components components, SquadContext context, Learner learner) {
        this.settings = checkNotNull(settings);
        this.components = checkNotNull(components);
        this.context = checkNotNull(context);
        this.learner = checkNotNull(learner);

        this.faulty = false;
        this.maxBallot = 0;
        this.resetRoundState(0);
    }

    /**
     * Set fields for synod to their initial value.
     *
     * @param instanceId set currentInstanceId to this value
     */
    private void resetRoundState(long instanceId) {
        this.acceptedValue = Event.BallotValue.EMPTY;
        this.acceptedBallot = 0;
        this.currentInstanceId = instanceId;
    }

    public boolean isFaulty() {
        return this.faulty;
    }

    private void setFaulty(){
        this.faulty = true;
        this.components.getJaxosMetrics().getOrCreateSquadMetrics(this.context.squadId()).setNormalStatus(false);
    }

    /**
     * Restore the inner state from the given instance object. Normally, it's called to restore loaded state from DB.
     *
     * @param instance not null
     */
    public void restore(Instance instance) {
        checkNotNull(instance);
        this.currentInstanceId = instance.id();
        this.maxBallot = instance.proposal();
        this.acceptedBallot = instance.proposal();
        this.acceptedValue = instance.value();
    }

    /**
     * Handle a PREPARE request
     *
     * @param request the PREPARE message
     * @return the response for this request, or null if self is faulty
     */
    public Event.PrepareResponse prepare(Event.PrepareRequest request) {
        checkNotNull(request, "The prepare request is null");

        if (logger.isTraceEnabled()) {
            logger.trace("S{}: On prepare {} ", context.squadId(), request);
        }

        Event.PrepareResponse resp = null;
        if (!this.faulty) {
            resp = doPrepare(request);
        }

        if (logger.isTraceEnabled()) {
            this.traceState();
            logger.trace("S{}: Gen {} ", context.squadId(), resp);
        }

        return resp;
    }

    /**
     * Handling prepare request actually
     */
    private Event.PrepareResponse doPrepare(Event.PrepareRequest request) {
        long last = handleAcceptedNotifyLostMaybe(this.context.chosenInstanceId(), request.senderId(), request.instanceId(), request.chosenInfo());

        if (request.instanceId() <= last) {
            logger.debug("S{}: PrepareResponse: historic prepare(instance id = {}), while my instance id is {} ",
                    context.squadId(), request.instanceId(), last);
            return outdatedPrepareResponse(request);
        }
        else if (request.instanceId() > last + 1) {
            logger.warn("S{}: PrepareResponse: future instance id in prepare(instance id = {}), last instance id = {}",
                    context.squadId(), request.instanceId(), last);
            return standByPrepareResponse(request);
        }
        else { // request.instanceId == last + 1
            if (this.currentInstanceId != request.instanceId()) { //the last instance may be changed by learn events
                this.resetRoundState(request.instanceId());
            }
            boolean success = false;
            int b0 = this.maxBallot;
            if (request.ballot() > this.maxBallot) {
                this.maxBallot = request.ballot();
                success = true;
                this.components.getLogger().saveInstance(this.context.squadId(), request.instanceId(), request.ballot(), this.acceptedValue);
            }

            if (!success && logger.isDebugEnabled()) {
                logger.debug("S{}: Reject prepare I {} ballot = {} while my max ballot = {}",
                        context.squadId(), request.instanceId(), request.ballot(), this.maxBallot);
            }

            return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                    .setResult(success ? Event.RESULT_SUCCESS : Event.RESULT_REJECT)
                    .setMaxProposal(b0)
                    .setAccepted(this.acceptedBallot, this.acceptedValue)
                    .setChosenInfo(this.context.getLastChosenInfo())
                    .build();
        }
    }

    /**
     * Create a standby response for given request
     */
    private Event.PrepareResponse standByPrepareResponse(Event.PrepareRequest request) {
        return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                .setResult(Event.RESULT_STANDBY)
                .setMaxProposal(0)
                .setAccepted(0, Event.BallotValue.EMPTY)
                .setChosenInfo(this.context.getLastChosenInfo())
                .build();
    }

    /**
     * Create a response for the PREPARE of an old accepted instance
     */
    private Event.PrepareResponse outdatedPrepareResponse(Event.PrepareRequest request) {
        Instance i0 = this.components.getLogger().loadInstance(this.context.squadId(), request.instanceId());
        return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                .setResult(Event.RESULT_REJECT)
                .setMaxProposal(Integer.MAX_VALUE)
                .setAccepted(i0.isEmpty() ? Integer.MAX_VALUE : i0.proposal(), i0.value())
                .setChosenInfo(this.context.getLastChosenInfo())
                .build();
    }

    /**
     * Handle ACCEPT request
     *
     * @return the response for the given request, or null if self is faulty
     */
    public Event.AcceptResponse accept(Event.AcceptRequest request) {
        checkNotNull(request);

        if (logger.isTraceEnabled()) {
            logger.trace("S{} On Accept {}", context.squadId(), request);
        }

        Event.AcceptResponse resp = null;
        if (!this.faulty) {
            resp = doAccept(request);
        }

        if (logger.isTraceEnabled()) {
            this.traceState();
            logger.trace("S{}: Gen {} ", context.squadId(), resp);
        }

        return resp;
    }

    /**
     * Handle ACCEPT request actually
     */
    private Event.AcceptResponse doAccept(Event.AcceptRequest request) {
        long last = handleAcceptedNotifyLostMaybe(this.context.chosenInstanceId(), request.senderId(), request.instanceId(), request.chosenInfo());

        if (request.instanceId() <= last) {
            if (logger.isDebugEnabled()) {
                logger.debug("S{}: AcceptResponse: historical in accept(instance id = {}), while my instance id is {} ",
                        context.squadId(), request.instanceId(), last);
            }
            return buildAcceptResponse(request, Integer.MAX_VALUE, Event.RESULT_REJECT);
        }
        else {
            if (this.currentInstanceId != request.instanceId()) { //the last instance may be changed by learn events
                this.resetRoundState(request.instanceId());
            }

            if (request.instanceId() > last + 1) {
                acceptValueMaybe(request);
                if (logger.isDebugEnabled()) {
                    logger.debug("S{}: AcceptResponse: future in accept(instance id = {}), request instance id = {}",
                            context.squadId(), last, request.instanceId());
                }
                return buildAcceptResponse(request, 0, Event.RESULT_STANDBY);
            }
            else { // request.instanceId == last + 1
                if (acceptValueMaybe(request)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("S{}: Accept new value sender = {}, instance = {}, ballot = {}, value = {}",
                                context.squadId(), request.senderId(), request.instanceId(), acceptedBallot, acceptedValue);
                    }
                    return buildAcceptResponse(request, this.maxBallot, Event.RESULT_SUCCESS);
                }
                else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("S{}: Reject accept {}  ballot = {}, while my maxBallot={}",
                                context.squadId(), request.instanceId(), request.ballot(), this.maxBallot);
                    }
                    return buildAcceptResponse(request, this.maxBallot, Event.RESULT_REJECT);
                }
            }
        }
    }

    private void traceState() {
        logger.trace("S{} currentInstanceId={}, maxBallot={}, acceptedBallot={}, acceptedValue={}",
                context.squadId(), currentInstanceId, maxBallot, acceptedBallot, acceptedValue);
    }

    private boolean acceptValueMaybe(Event.AcceptRequest request) {
        if (request.ballot() >= this.maxBallot) {
            this.acceptedBallot = this.maxBallot = request.ballot();
            this.acceptedValue = request.value();
            this.components.getLogger().saveInstance(this.context.squadId(), request.instanceId(), this.maxBallot, this.acceptedValue);
            return true;
        }
        return false;
    }

    private Event.AcceptResponse buildAcceptResponse(Event.AcceptRequest request, int proposal, int result) {
        return new Event.AcceptResponse(settings.serverId(), this.context.squadId(), request.instanceId(), request.round(),
                proposal, result, this.acceptedValue.id(), this.context.getLastChosenInfo());
    }

    private long handleAcceptedNotifyLostMaybe(long chosenInstanceId, int proposer, long requestInstanceId, Event.ChosenInfo chosenInfo) {
        //When Notify message lost, the critical numbers may look like:
        //my chosenInstanceId is 10001
        //requestInstanceId from PREPARE or ACCEPT is 10003, and curried ChosenInfo.instanceId = 10002
        //the currentInstanceId is 10002
        if (requestInstanceId == chosenInstanceId + 2) {
            if (this.currentInstanceId == chosenInfo.instanceId() && this.acceptedBallot > 0 && this.acceptedValue.id() == chosenInfo.ballotId()) {
                logger.info("S{}: success handle notify lost, when handle prepare({}), mine is {}",
                        context.squadId(), requestInstanceId, chosenInstanceId);

                chose(proposer, this.currentInstanceId, this.acceptedBallot);
            }
        }
        return this.context.chosenInstanceId();
    }

    public void onChosenNotify(Event.ChosenNotification notification) {
        checkNotNull(notification, "Given notification parameter  is null");

        if (logger.isTraceEnabled()) {
            logger.trace("S{}: NOTIFY receive chose notify {}, value = {}",
                    context.squadId(), notification, this.acceptedValue);
        }
        if (notification.ballot() <= 0) {
            logger.warn("S{}: receive illegal ballot number {}", context.squadId(), notification);
            return;
        }

        if (this.faulty) {
            return;
        }

        if (this.currentInstanceId != notification.instanceId()) {
            if (logger.isDebugEnabled()) {
                logger.debug("S{}: got mismatched chosen notify of instance {} while mine is {}",
                        context.squadId(), notification.instanceId(), this.currentInstanceId);
            }
            return;
        }

        long last = this.context.chosenInstanceId();
        if (notification.instanceId() == last + 1) {
            if (notification.ballotId() == this.acceptedValue.id()) {
                chose(notification.senderId(), notification.instanceId(), notification.ballot());
            }
            else if (this.acceptedBallot > 0) {
                //ignore this instance, let future learn recover it
                String s0 = Long.toHexString(this.acceptedValue.id()).toUpperCase();
                String s1 = Long.toHexString(notification.ballotId()).toUpperCase();
                logger.warn("S{} I{} Got NOTIFY event with different message id {}, mine is {}  ", context.squadId(),
                        notification.instanceId(), s1, s0);
            }
            // else case is this.acceptedBallotId == 0, it means no accepted value
        }
        else {
            logger.debug("S{}: Got NOTIFY message of mismatched instance({}), while my last instance id is {} ",
                    context.squadId(), notification.instanceId(), last);
        }
    }

    private void chose(int proposer, long instanceId, int proposal) {
        try {
            learner.learnValue(new Instance(this.context.squadId(), instanceId, proposal, this.acceptedValue));
        }
        catch (Exception e) {
            setFaulty();
            String msg = String.format("Error when chosen value %d.%d", this.context.squadId(), instanceId);
            logger.error(msg, e);
        }

        context.recordChosenInfo(proposer, instanceId, this.acceptedValue.id(), proposal);
        // for multi paxos, prepare once and accept many, keep maxBallot unchanged
        // this.maxBallot = unchanged
        this.resetRoundState(0);
    }

}
