package org.axesoft.jaxos.algo;

public interface Learner {
    void learnValue(Instance i);

    Instance getLastChosenInstance(int squadId);
}
