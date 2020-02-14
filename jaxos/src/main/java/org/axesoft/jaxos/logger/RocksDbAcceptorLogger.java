package org.axesoft.jaxos.logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * A paxos logger implementation based on the RocksDB and ProtoBuff message coder
 */
public class RocksDbAcceptorLogger implements AcceptorLogger {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger logger = LoggerFactory.getLogger(RocksDbAcceptorLogger.class);

    private static final int KEEP_OLD_LOG_NUM = 10000;

    private static final byte CATEGORY_LAST = 1;
    private static final byte CATEGORY_PROMISE = 2;
    private static final byte CATEGORY_CHECKPOINT = 3;

    private static final byte[] EMPTY_BYTES = new byte[]{};
    private static final byte[] KEY_OF_LAST_INSTANCE = new byte[]{CATEGORY_LAST};
    private static final byte[] KEY_OF_CHECKPOINT = new byte[]{CATEGORY_CHECKPOINT};

    private ProtoMessageCoder messageCoder;
    private String path;
    private int squadCount;

    private DBOptions dbOptions;
    private RocksDB db;
    private ColumnFamilyHandle defaultHandle;
    private Map<Integer, ColumnFamilyHandle> handleMap;
    private boolean alwaysSync;
    private JaxosMetrics metrics;


    public RocksDbAcceptorLogger(String path, int squadCount, boolean alwaysSync, JaxosMetrics metrics) {
        checkDbIfExist(path, squadCount);

        this.squadCount = squadCount;
        this.path = path;
        this.messageCoder = new ProtoMessageCoder();
        this.metrics = metrics;
        this.alwaysSync = alwaysSync;

        tryCreateDir(path);
        this.dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setStatistics(new Statistics())
                .setMaxBackgroundCompactions(4)
                .setMaxBackgroundFlushes(2)
                .setBytesPerSync(2 * 1048576)
                .setMaxTotalWalSize(300 * 1048576)
                .setManualWalFlush(!alwaysSync);

        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(new ColumnFamilyDescriptor(("default").getBytes(StandardCharsets.UTF_8)));
        for (int i = 0; i < squadCount; i++) {
            ColumnFamilyOptions opt = new ColumnFamilyOptions()
                    .setLevelCompactionDynamicLevelBytes(true);
            ColumnFamilyDescriptor desc = new ColumnFamilyDescriptor(("S" + i).getBytes(StandardCharsets.UTF_8), opt);
            descriptors.add(desc);
        }

        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try {
            db = RocksDB.open(this.dbOptions, path, descriptors, handles);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        this.defaultHandle = handles.get(0);

        ImmutableMap.Builder<Integer, ColumnFamilyHandle> builder = ImmutableMap.builder();
        for (int i = 0; i < squadCount; i++) {
            builder.put(i, handles.get(i + 1));
        }
        this.handleMap = builder.build();
    }

    private void checkDbIfExist(String path, int squadCount) {
        File f = new File(path + "/CURRENT");
        if (!f.exists()) {
            return;
        }

        Set<String> columnFamilies = new HashSet<>();
        try {
            List<byte[]> bx = RocksDB.listColumnFamilies(new Options(), path);
            for (byte[] bytes : bx) {
                columnFamilies.add(new String(bytes, StandardCharsets.UTF_8));
            }
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        if (columnFamilies.size() != squadCount + 1) {
            String msg = String.format("DBError Previous squad count is %d, unequal to new squad count %d",
                    columnFamilies.size() - 1, squadCount);
            throw new IllegalArgumentException(msg);
        }

        for (int i = 0; i < squadCount; i++) {
            if (!columnFamilies.contains("S" + i)) {
                throw new RuntimeException("DBError wanted column family S" + i + " not exist");
            }
        }
    }

    private ColumnFamilyHandle getHandle(int squadId) {
        ColumnFamilyHandle h = this.handleMap.get(squadId);
        if (h == null) {
            throw new RuntimeException("ColumnFamilyHandle for " + squadId + " not exist");
        }
        return h;
    }

    private void tryCreateDir(String path) {
        File f = new File(path);
        if (f.exists()) {
            if (!f.isDirectory()) {
                throw new RuntimeException(path + " is not a directory");
            }
        }
        else {
            if (!f.mkdir()) {
                throw new RuntimeException("can not mkdir at " + path);
            }
        }
    }

    @Override
    public void saveInstance(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        byte[] instanceKey = keyOfInstance(instanceId);
        byte[] data = toByteArray(new Instance(squadId, instanceId, proposal, value));

        long t0 = System.nanoTime();
        ColumnFamilyHandle handle = getHandle(squadId);
        try (final WriteBatch writeBatch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions().setSync(this.alwaysSync)) {
            writeBatch.put(handle, instanceKey, data);
            writeBatch.put(handle, KEY_OF_LAST_INSTANCE, instanceKey);
            RocksDbAcceptorLogger.this.db.write(writeOptions, writeBatch);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        RocksDbAcceptorLogger.this.metrics.recordLoggerSaveElapsed(System.nanoTime() - t0);
    }

    @Override
    public Instance loadLastInstance(int squadId) {
        long t0 = System.nanoTime();
        ColumnFamilyHandle handle = getHandle(squadId);
        try {
            byte[] key = db.get(handle, KEY_OF_LAST_INSTANCE);
            if (key == null) {
                return Instance.emptyOf(squadId);
            }
            byte[] value = db.get(handle, key);
            if (value == null) {
                return Instance.emptyOf(squadId);
            }
            return toInstance(value);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        finally {
            this.metrics.recordLoggerLoadElapsed(System.nanoTime() - t0);
        }
    }


    @Override
    public Instance loadInstance(int squadId, long instanceId) {
        long t0 = System.nanoTime();
        try {
            byte[] bx = db.get(getHandle(squadId), keyOfInstance(instanceId));
            if (bx == null) {
                return Instance.emptyOf(squadId);
            }
            return toInstance(bx);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        finally {
            this.metrics.recordLoggerLoadElapsed(System.nanoTime() - t0);
        }
    }

    @Override
    public void saveCheckPoint(CheckPoint checkPoint, boolean deleteOldInstances) {
        long t0 = System.nanoTime();
        byte[] data = toByteArray(checkPoint);
        try (final WriteOptions writeOptions = new WriteOptions().setSync(this.alwaysSync)) {
            db.put(getHandle(checkPoint.squadId()), writeOptions, KEY_OF_CHECKPOINT, data);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        this.metrics.recordLoggerSaveCheckPointElapse(System.nanoTime() - t0);

//        if(checkPoint.squadId() == this.squadCount -1 ) {
//            logger.info(this.dbOptions.statistics().toString());
//        }

        if (deleteOldInstances && checkPoint.instanceId() > KEEP_OLD_LOG_NUM) {
            byte[] lowKey = keyOfInstance(0);
            byte[] highKey = keyOfInstance(checkPoint.instanceId() - KEEP_OLD_LOG_NUM);
            deleteRange(checkPoint.squadId(), lowKey, highKey);
        }
    }

    private void deleteRange(int squadId, byte[] low, byte[] high) {
        long t0 = System.nanoTime();
        try (final WriteBatch writeBatch = new WriteBatch();
             final WriteOptions opt = new WriteOptions().setSync(this.alwaysSync)) {
            writeBatch.deleteRange(getHandle(squadId), low, high);
            db.write(opt, writeBatch);
            db.compactRange();
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        this.metrics.recordLoggerDeleteElapsed((System.nanoTime() - t0));
    }

    @Override
    public CheckPoint loadLastCheckPoint(int squadId) {
        byte[] data;
        try {
            data = db.get(getHandle(squadId), KEY_OF_CHECKPOINT);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        if (data == null) {
            return CheckPoint.EMPTY;
        }
        return toCheckPoint(data);
    }

    @Override
    public void sync() {
        long t0 = System.nanoTime();

        try {
            db.flushWal(true);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        this.metrics.recordLoggerSyncElapsed(System.nanoTime() - t0);
    }

    @Override
    public void close() {
        try {
            sync();
            this.db.closeE();
        }
        catch (RocksDBException e) {
            logger.error("Error when close db in " + this.path);
        }
    }

    private byte[] toByteArray(CheckPoint checkPoint) {
        PaxosMessage.CheckPoint c = this.messageCoder.encodeCheckPoint(checkPoint);
        return c.toByteArray();
    }

    private CheckPoint toCheckPoint(byte[] bytes) {
        try {
            PaxosMessage.CheckPoint checkPoint = PaxosMessage.CheckPoint.parseFrom(bytes);
            return this.messageCoder.decodeCheckPoint(checkPoint);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private Instance toInstance(byte[] bytes) {
        PaxosMessage.Instance i;
        try {
            i = PaxosMessage.Instance.parseFrom(bytes);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        Event.BallotValue v = messageCoder.decodeValue(i.getValue());
        return new Instance(i.getSquadId(), i.getInstanceId(), i.getProposal(), v);
    }


    private byte[] toByteArray(Instance v) {
        return PaxosMessage.Instance.newBuilder()
                .setSquadId(v.squadId())
                .setInstanceId(v.id())
                .setProposal(v.proposal())
                .setValue(messageCoder.encodeValue(v.value()))
                .build()
                .toByteArray();
    }

    private byte[] keyOfInstance(long i) {
        byte[] key = new byte[9];
        key[0] = CATEGORY_PROMISE;
        System.arraycopy(Longs.toByteArray(i), 0, key, 1, 8);
        return key;
    }
}
