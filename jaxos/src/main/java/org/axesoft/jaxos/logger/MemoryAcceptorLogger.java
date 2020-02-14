package org.axesoft.jaxos.logger;

import org.axesoft.jaxos.algo.AcceptorLogger;
import org.axesoft.jaxos.algo.CheckPoint;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.Instance;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author bison
 * @sine 2020/1/10.
 */
public class MemoryAcceptorLogger implements AcceptorLogger {
    private ConcurrentMap<Integer, InstanceValueRingCache> cacheMap = new ConcurrentHashMap<>();
    private ConcurrentMap<Integer, CheckPoint> checkPointMap = new ConcurrentHashMap<>();

    private int cacheSize;

    public MemoryAcceptorLogger(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    private InstanceValueRingCache getCache(int squadId){
        return cacheMap.computeIfAbsent(squadId, i -> new InstanceValueRingCache(cacheSize));
    }
    @Override
    public void saveInstance(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        getCache(squadId).put(new Instance(squadId, instanceId, proposal, value));
    }

    @Override
    public Instance loadLastInstance(int squadId) {
        return getCache(squadId).getLast().orElse(Instance.emptyOf(squadId));
    }

    @Override
    public Instance loadInstance(int squadId, long instanceId) {
        List<Instance> ix = getCache(squadId).get(instanceId, instanceId);
        return ix.isEmpty()? Instance.emptyOf(squadId) : ix.get(0);
    }

    @Override
    public void saveCheckPoint(CheckPoint checkPoint, boolean deleteOldInstances) {
        checkPointMap.put(checkPoint.squadId(), checkPoint);
    }

    @Override
    public CheckPoint loadLastCheckPoint(int squadId) {
        return checkPointMap.computeIfAbsent(squadId, k -> CheckPoint.EMPTY);
    }

    @Override
    public void sync() {

    }

    @Override
    public void close() {

    }
}
