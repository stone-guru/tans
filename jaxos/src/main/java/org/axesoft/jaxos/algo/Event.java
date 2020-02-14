package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


public abstract class Event {
    public enum Code {
        NOOP, HEART_BEAT, HEART_BEAT_RESPONSE,
        PROPOSAL_TIMEOUT,
        PREPARE, PREPARE_RESPONSE, PREPARE_TIMEOUT,
        ACCEPT, ACCEPT_RESPONSE, ACCEPT_TIMEOUT,
        ACCEPTED_NOTIFY, ACCEPTED_NOTIFY_RESPONSE,
        LEARN_REQUEST, LEARN_RESPONSE, LEARN_TIMEOUT,
        CHOSEN_QUERY, CHOSEN_QUERY_RESPONSE, CHOSEN_QUERY_TIMEOUT
    }

    //FIXME refact to enum
    public static final int RESULT_REJECT = 0;
    public static final int RESULT_SUCCESS = 1;
    public static final int RESULT_STANDBY = 3;

    private int senderId;
    private long timestamp;

    public Event(int sender) {
        this(sender, System.currentTimeMillis());
    }

    public Event(int senderId, long timestamp) {
        this.senderId = senderId;
        this.timestamp = timestamp;
    }

    abstract public Code code();

    public int senderId() {
        return this.senderId;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public int squadId() {
        return -1;
    }

    public static class HeartBeatRequest extends Event {
        public HeartBeatRequest(int senderId) {
            super(senderId);
        }

        public HeartBeatRequest(int senderId, long timestamp) {
            super(senderId, timestamp);
        }

        @Override
        public Code code() {
            return Code.HEART_BEAT;
        }
    }

    public static class HeartBeatResponse extends Event {
        public HeartBeatResponse(int senderId, long timestamp) {
            super(senderId, timestamp);
        }

        public HeartBeatResponse(int senderId) {
            super(senderId);
        }

        @Override
        public Code code() {
            return Code.HEART_BEAT_RESPONSE;
        }
    }


    public static abstract class BallotEvent extends Event {
        private int squadId;
        private long instanceId;
        private int round;

        public BallotEvent(int senderId, int squadId, long instanceId, int round) {
            super(senderId);
            this.squadId = squadId;
            this.instanceId = instanceId;
            this.round = round;
        }

        @Override
        public int squadId() {
            return this.squadId;
        }

        public long instanceId() {
            return this.instanceId;
        }

        public int round() {
            return this.round;
        }

        public ChosenInfo chosenInfo(){
            return null;
        }

        @Override
        public String toString() {
            return "senderId=" + super.senderId +
                    ", squadId=" + squadId +
                    ", instanceId=" + instanceId +
                    ", round=" + round;
        }
    }

    public enum ValueType {
        NOTHING(0), APPLICATION(1), NOOP(2);

        private int code;

        ValueType(int code) {
            this.code = code;
        }

        public int code() {
            return this.code;
        }

        public static ValueType fromCode(int code) {
            switch (code) {
                case 0:
                    return NOTHING;
                case 1:
                    return APPLICATION;
                case 2:
                    return NOOP;
                default:
                    throw new IllegalArgumentException("Unknown code " + code);
            }
        }
    }

    public static class BallotValue {
        public static final BallotValue EMPTY = new BallotValue(0, ValueType.NOTHING, ByteString.EMPTY);

        public static final BallotValue NOOP = new BallotValue(0, ValueType.NOOP, ByteString.EMPTY);

        public static final BallotValue appValue(long id, ByteString v) {
            return new BallotValue(id, ValueType.APPLICATION, v);
        }

        private long id;
        private ValueType type;
        private ByteString content;

        public BallotValue(long id, ValueType type, ByteString content) {
            this.id = id;
            this.type = type;
            this.content = content;
        }

        public long id(){
            return this.id;
        }

        public ValueType type() {
            return this.type;
        }

        public ByteString content() {
            return this.content;
        }

        @Override
        public String toString() {
            return "BallotValue{" +  Long.toHexString(id).toUpperCase()  + ", " + type + ", b[" + content.size() + "]}";
        }
    }

    public static class ChosenInfo {
        private long instanceId;
        private long ballotId;
        private long elapsedMillis;

        public ChosenInfo(long instanceId, long ballotId, long elapsedMillis) {
            this.instanceId = instanceId;
            this.ballotId = ballotId;
            this.elapsedMillis = elapsedMillis;
        }

        public long instanceId(){
            return this.instanceId;
        }

        public long ballotId(){
            return this.ballotId;
        }

        public long elapsedMillis(){
            return this.elapsedMillis;
        }

        @Override
        public String toString() {
            return "ChosenInfo{" +
                    "instanceId=" + instanceId +
                    ", ballotId=" + ballotId +
                    ", elapsedMillis=" + elapsedMillis +
                    '}';
        }
    }

    public static class PrepareRequest extends BallotEvent {
        private int ballot;
        private ChosenInfo chosenInfo;

        public PrepareRequest(int sender, int squadId, long instanceId, int round, int ballot, ChosenInfo chosenInfo) {
            super(sender, squadId, instanceId, round);
            this.ballot = ballot;
            this.chosenInfo = chosenInfo;
        }

        @Override
        public Code code() {
            return Code.PREPARE;
        }


        public int ballot() {
            return ballot;
        }

        @Override
        public ChosenInfo chosenInfo() {
            return this.chosenInfo;
        }

        @Override
        public String toString() {
            return "PrepareRequest{" + super.toString() +
                    ", ballot=" + this.ballot +
                    ", " + chosenInfo +
                    '}';
        }
    }


    public static class PrepareResponse extends BallotEvent {
        private int result;
        private int maxBallot;
        private int acceptedBallot;
        private BallotValue acceptedValue;
        private ChosenInfo chosenInfo;

        public static class Builder {
            private PrepareResponse resp;

            public Builder(int sender, int squadId, long instanceId, int round) {
                resp = new PrepareResponse(sender, squadId, instanceId, round);
            }

            public Builder setResult(int result) {
                resp.result = result;
                return this;
            }

            public Builder setMaxProposal(int proposal) {
                resp.maxBallot = proposal;
                return this;
            }

            public Builder setAccepted(int proposal, BallotValue value) {
                resp.acceptedBallot = proposal;
                resp.acceptedValue = checkNotNull(value);
                return this;
            }

            public Builder setChosenInfo(ChosenInfo chosenInfo) {
                resp.chosenInfo = chosenInfo;
                return this;
            }

            public PrepareResponse build() {
                return resp;
            }
        }

        private PrepareResponse(int sender, int squadId, long instanceId, int round) {
            super(sender, squadId, instanceId, round);
        }

        @Override
        public Code code() {
            return Code.PREPARE_RESPONSE;
        }

        public int result() {
            return this.result;
        }

        public int maxBallot() {
            return this.maxBallot;
        }

        public int acceptedBallot() {
            return this.acceptedBallot;
        }

        public BallotValue acceptedValue() {
            return this.acceptedValue;
        }

        @Override
        public ChosenInfo chosenInfo() {
            return this.chosenInfo;
        }

        @Override
        public String toString() {
            return "PrepareResponse{" + super.toString() +
                    ", result =" + result +
                    ", maxBallot=" + maxBallot +
                    ", acceptedBallot=" + acceptedBallot +
                    ", acceptedValue=" + acceptedValue +
                    ", " + chosenInfo +
                    '}';
        }
    }

    public static class AcceptRequest extends BallotEvent {
        private int ballot;
        private BallotValue value;
        private ChosenInfo chosenInfo;

        public static Builder newBuilder(int sender, int squadId, long instanceId, int round) {
            return new Builder(sender, squadId, instanceId, round);
        }

        public static class Builder {
            private AcceptRequest req;

            public Builder(int sender, int squadId, long instanceId, int round) {
                req = new AcceptRequest(sender, squadId, instanceId, round);
            }

            public Builder setBallot(int ballot) {
                req.ballot = ballot;
                return this;
            }

            public Builder setValue(BallotValue value) {
                req.value = value;
                return this;
            }

            public Builder setChosenInfo(ChosenInfo chosenInfo){
                req.chosenInfo = chosenInfo;
                return this;
            }

            public AcceptRequest build() {
                return this.req;
            }
        }

        private AcceptRequest(int sender, int squadId, long instanceId, int round) {
            super(sender, squadId, instanceId, round);
        }

        @Override
        public Code code() {
            return Code.ACCEPT;
        }

        public int ballot() {
            return this.ballot;
        }

        public BallotValue value() {
            return this.value;
        }

        @Override
        public ChosenInfo chosenInfo() {
            return this.chosenInfo;
        }

        @Override
        public String toString() {
            return "AcceptRequest{" + super.toString() +
                    ", ballot=" + ballot +
                    ", value=" + value +
                    ", " + chosenInfo +
                    '}';
        }
    }

    public static class AcceptResponse extends BallotEvent {
        private int maxBallot;
        private int result;
        private long acceptedBallotId;
        private ChosenInfo chosenInfo;

        public AcceptResponse(int sender, int squadId, long instanceId, int round, int maxBallot, int result, long acceptedBallotId, ChosenInfo chosenInfo) {
            super(sender, squadId, instanceId, round);
            this.maxBallot = maxBallot;
            this.result = result;
            this.acceptedBallotId = acceptedBallotId;
            this.chosenInfo = chosenInfo;
        }

        @Override
        public Code code() {
            return Code.ACCEPT_RESPONSE;
        }

        public int maxBallot() {
            return this.maxBallot;
        }

        public int result() {
            return this.result;
        }

        public long acceptedBallotId(){
            return this.acceptedBallotId;
        }

        @Override
        public ChosenInfo chosenInfo() {
            return this.chosenInfo;
        }

        @Override
        public String toString() {
            return "AcceptResponse{" + super.toString() +
                    ", maxBallot=" + maxBallot +
                    ", result=" + result +
                    ", " + chosenInfo +
                    '}';
        }
    }

    public static class ChosenNotify extends BallotEvent {
        private int ballot;
        private long ballotId;

        public ChosenNotify(int sender, int squadId, long instanceId, int ballot, long ballotId) {
            super(sender, squadId, instanceId, 0);
            this.ballot = ballot;
            this.ballotId = ballotId;
        }

        @Override
        public Code code() {
            return Code.ACCEPTED_NOTIFY;
        }

        public int ballot() {
            return this.ballot;
        }

        public long ballotId(){
            return this.ballotId;
        }

        @Override
        public String toString() {
            return "AcceptedNotify{" + super.toString() +
                    ", ballot=" + ballot +
                    ", ballotId=" + Long.toHexString(ballotId).toUpperCase() +
                    '}';
        }
    }

    public static class PrepareTimeout extends BallotEvent {
        public PrepareTimeout(int senderId, int squadId, long instanceId, int round) {
            super(senderId, squadId, instanceId, round);
        }

        @Override
        public Code code() {
            return Code.PREPARE_TIMEOUT;
        }


        @Override
        public String toString() {
            return "PrepareTimeout{" + super.toString() + "}";
        }
    }

    public static class AcceptTimeout extends BallotEvent {
        public AcceptTimeout(int senderId, int squadId, long instanceId, int round) {
            super(senderId, squadId, instanceId, round);
        }

        @Override
        public Code code() {
            return Code.ACCEPT_TIMEOUT;
        }

        @Override
        public String toString() {
            return "AcceptTimeout{" + super.toString() + "}";
        }
    }

    public static class ProposalTimeout extends BallotEvent {
        public ProposalTimeout(int senderId, int squadId, long instanceId, int round) {
            super(senderId, squadId, instanceId, round);
        }

        @Override
        public Code code() {
            return Code.PROPOSAL_TIMEOUT;
        }

        @Override
        public String toString() {
            return "ProposalTimeout{" +
                    "squadId=" + super.squadId +
                    ", instanceId=" + super.instanceId +
                    ", timestamp=" + new Date(super.timestamp()) +
                    '}';
        }
    }

    public static abstract class InstanceEvent extends Event {
        public InstanceEvent(int senderId) {
            super(senderId);
        }
    }

    public static class Learn extends InstanceEvent {
        private int squadId;
        private long highInstanceId;
        private long lowInstanceId;

        public Learn(int senderId, int squadId, long lowInstanceId, long highInstanceId) {
            super(senderId);
            this.squadId = squadId;
            this.highInstanceId = highInstanceId;
            this.lowInstanceId = lowInstanceId;
        }

        @Override
        public Code code() {
            return Code.LEARN_REQUEST;
        }

        @Override
        public int squadId() {
            return this.squadId;
        }

        public long lowInstanceId() {
            return this.lowInstanceId;
        }

        public long highInstanceId() {
            return this.highInstanceId;
        }

        @Override
        public String toString() {
            return "Learn{" +
                    "senderId=" + super.senderId() +
                    ", squadId=" + this.squadId +
                    ", lowInstanceId=" + this.lowInstanceId +
                    ", highInstanceId=" + this.highInstanceId +
                    '}';
        }
    }

    public static class LearnResponse extends InstanceEvent {
        private int squadId;
        private List<Instance> instances;
        private CheckPoint checkPoint;
        public LearnResponse(int senderId, int squadId, List<Instance> instances, CheckPoint checkPoint) {
            super(senderId);
            this.squadId = squadId;
            this.instances = checkNotNull(instances);
            this.checkPoint = checkNotNull(checkPoint);
        }

        @Override
        public Code code() {
            return Code.LEARN_RESPONSE;
        }

        @Override
        public int squadId() {
            return this.squadId;
        }

        public List<Instance> instances() {
            return this.instances;
        }

        public CheckPoint checkPoint(){
            return this.checkPoint;
        }

        @Override
        public String toString() {
            return "LearnResponse{" +
                    "senderId=" + super.senderId() +
                    ", squadId=" + this.squadId +
                    ", instances=I[" + this.instances.size() + "]" +
                    ", checkPoint=B[" + this.checkPoint.content().size() + "]" +
                    '}';
        }

        public long lowInstanceId() {
            return instanceIdOf(0);
        }

        public long highInstanceId() {
            return instanceIdOf(instances.size() - 1);
        }

        private long instanceIdOf(int index) {
            if (index >= 0 && index < instances.size()) {
                return instances.get(index).id();
            }
            return 0;
        }
    }

    public static class LearnTimeout extends InstanceEvent {
        private int squadId;

        public LearnTimeout(int senderId, int squadId) {
            super(senderId);
            this.squadId = squadId;
        }

        @Override
        public Code code() {
            return Code.LEARN_TIMEOUT;
        }

        @Override
        public int squadId() {
            return this.squadId;
        }
    }

    public static class ChosenQuery extends InstanceEvent {
        public ChosenQuery(int senderId) {
            super(senderId);
        }

        @Override
        public Code code() {
            return Code.CHOSEN_QUERY;
        }
    }

    public static class ChosenQueryResponse extends InstanceEvent {
        List<Pair<Integer, Long>> squadChosen;

        public ChosenQueryResponse(int senderId, List<Pair<Integer, Long>> squadChosen) {
            super(senderId);
            this.squadChosen = squadChosen;
        }

        public List<Pair<Integer, Long>> squadChosen() {
            return squadChosen;
        }

        public long chosenInstanceIdOf(int squadId) {
            for (Pair<Integer, Long> p : squadChosen) {
                if (p.getLeft() == squadId) {
                    return p.getRight();
                }
            }
            return 0;
        }

        @Override
        public Code code() {
            return Code.CHOSEN_QUERY_RESPONSE;
        }
    }

    public static class ChosenQueryTimeout extends InstanceEvent {

        public ChosenQueryTimeout(int senderId) {
            super(senderId);
        }

        @Override
        public Code code() {
            return Code.CHOSEN_QUERY_TIMEOUT;
        }
    }
}
