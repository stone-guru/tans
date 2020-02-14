package org.axesoft.jaxos;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.time.StopWatch;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.logger.MemoryAcceptorLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;


public class AcceptorLoggerTest {
    final static String DB_DIR = "/tmp/jaxosdb";
    private static AcceptorLogger logger;
    final static int n = 4000;

    @BeforeClass
    public static void setup(){
        File dir = new File(DB_DIR);
        if(!dir.exists()){
            dir.mkdir();
        }
        //logger = new LevelDbAcceptorLogger(DB_DIR, Duration.ofMillis(20), new DummyJaxosMetrics());
        logger = new MemoryAcceptorLogger(5000);

        StopWatch w = StopWatch.createStarted();
        for(int i = 0; i < n; i++) {
            logger.saveInstance(1, 1000 + i, 1 + i, Event.BallotValue.appValue(i, ByteString.copyFromUtf8("Hello" + i)));
            logger.saveInstance(2, 1000 + i, 1 + i, Event.BallotValue.appValue(i, ByteString.copyFromUtf8("Hello" + i)));
        }

        w.stop();
        double seconds = w.getTime(TimeUnit.MILLISECONDS)/1000.0;
        System.out.println(String.format("Insert %d records in %.2f s, OPS = %.2f", 2 * n, seconds, (2.0 * n)/seconds));
    }

    @AfterClass
    public static void shutdown(){
        logger.close();
    }

    @Test
    public void testLoadLast(){
        Instance p = logger.loadLastInstance(1);
        assertNotNull(p);

        assertEquals(1, p.squadId());
        assertEquals(1000 + n - 1, p.id());
        assertEquals(n, p.proposal());
        assertEquals("Hello" + (n - 1), p.value().content().toStringUtf8());
    }

    @Test
    public void testLoad()  {
        Instance p = logger.loadInstance(2, 1100);
        assertNotNull(p);

        assertEquals(2, p.squadId());
        assertEquals(1100, p.id());
        assertEquals(101, p.proposal());
        assertEquals("Hello100", p.value().content().toStringUtf8());
    }

    public static class DummyJaxosMetrics implements JaxosMetrics {

        @Override
        public SquadMetrics getOrCreateSquadMetrics(int squadId) {
            return null;
        }

        @Override
        public void recordRestoreElapsedMillis(long millis) {

        }

        @Override
        public void recordLoggerLoadElapsed(long nanos) {

        }

        @Override
        public void recordLoggerSaveElapsed(long nanos) {

        }

        @Override
        public void recordLoggerSyncElapsed(long nanos) {

        }

        @Override
        public void recordLoggerDeleteElapsed(long millis) {

        }

        @Override
        public void recordLoggerSaveCheckPointElapse(long millis) {

        }

        @Override
        public String format() {
            return null;
        }
    }
}
