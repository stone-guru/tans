package org.axesoft.jaxos.algo;

/**
 * @author gaoyuan
 * @sine 2019/9/9.
 */
public interface Learner {
    void learnValue(Instance i);

    Instance getLastChosenInstance(int squadId);
}
