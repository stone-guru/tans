package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StateMachineRunner implements Learner {
    private static Logger logger = LoggerFactory.getLogger(StateMachineRunner.class);

    private int squadId;
    private StateMachine machine;
    private Instance lastChosen;

    public StateMachineRunner(int squadId, StateMachine machine) {
        this.squadId = squadId;
        this.machine = checkNotNull(machine);
        this.lastChosen = Instance.emptyOf(squadId);
    }

    public synchronized void restoreFromCheckPoint(CheckPoint checkPoint, List<Instance> ix) {
        if (!checkPoint.isEmpty()) {
            if(checkPoint.squadId() != this.squadId){
                throw new IllegalArgumentException(checkPoint + " not match mine " + this.squadId );
            }
            if(checkPoint.instanceId() > this.lastChosen.id()) {
                this.machine.restoreFromCheckPoint(checkPoint.squadId(), checkPoint.instanceId(), checkPoint.content());
                this.lastChosen = checkPoint.lastInstance();
                logger.info("S{} accept checkpoint of {}", checkPoint.squadId(), checkPoint);
            }
        }

        Iterator<Instance> it = ix.iterator();
        Instance i1, i2 = null;
        while (it.hasNext()) {
            i1 = it.next();
            if(i1.id() > this.lastChosen.id()){
                i2 = i1;
                break;
            }
        }

        Instance i = i2;
        while (i != null) {
            this.innerLearn(i);
            i = it.hasNext()? it.next() : null;
        }

        if(i2 != null){
            logger.info("S{} accept {} instances from {} to {}", this.squadId, this.lastChosen.id() - i2.id() + 1, i2.id(), this.lastChosen.id());
        }
    }

    public synchronized CheckPoint makeCheckPoint() {
        long timestamp = System.currentTimeMillis();
        Pair<ByteString, Long> p = this.machine.makeCheckPoint(this.squadId);
        return new CheckPoint(this.squadId, p.getRight(), timestamp, p.getLeft(), this.lastChosen);
    }

    @Override
    public synchronized Instance getLastChosenInstance(int squadId) {
        checkArgument(squadId == this.squadId, "given squad id %d is unequal to mine %d", squadId, this.squadId);
        return this.lastChosen;
    }

    @Override
    public synchronized void learnValue(Instance i) {
        checkArgument(i.squadId() == this.squadId, "given squad id %d is unequal to mine %d", i.squadId(), this.squadId);
        long i0 = this.lastChosen.id();
        if (i.id() != i0 + 1) {
            throw new IllegalStateException(String.format("Learning ignore given instance %d, mine is %d", i.id(), i0));
        }
        innerLearn(i);
    }

    private void innerLearn(Instance i) {
        if (i.value().type() == Event.ValueType.APPLICATION) {
            this.machine.consume(squadId, i.id(), i.value().content());
        }
        this.lastChosen = i;
    }

}
