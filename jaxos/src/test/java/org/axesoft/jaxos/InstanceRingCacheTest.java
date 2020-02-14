package org.axesoft.jaxos;

import com.google.protobuf.ByteString;
import io.netty.util.CharsetUtil;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.Instance;
import org.axesoft.jaxos.logger.InstanceValueRingCache;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author gaoyuan
 * @sine 2019/10/15.
 */
public class InstanceRingCacheTest {
    private Instance newValue(long instanceId) {
        return new Instance(1, instanceId, 2,
                Event.BallotValue.appValue(instanceId, ByteString.copyFrom(Long.toString(instanceId), CharsetUtil.UTF_8)));
    }

    @Test
    public void testSize() {
        InstanceValueRingCache cache = new InstanceValueRingCache(256);

        assertEquals(0, cache.size());

        int i0 = 10000;
        for (int i = i0; i < i0 + 256; i++) {
            cache.put(newValue(i));
            assertEquals(i - i0 + 1, cache.size());
        }

        for (int j = 0; j < 10; j++) {
            cache.put(newValue(i0 + 256 + j));
            assertEquals(256, cache.size());
        }
    }

    @Test
    public void testGet1() {
        InstanceValueRingCache cache = new InstanceValueRingCache(256);
        long i0 = 10000;
        for (long i = i0; i < i0 + 128; i++) {
            cache.put(newValue(i));
        }

        List<Instance> ix1 = cache.get(i0, i0 + 9);
        assertEquals(10, ix1.size());
        assertEquals(i0, ix1.get(0).id());
        assertEquals(i0 + 9, ix1.get(9).id());
    }


    @Test
    public void testGet2() {
        InstanceValueRingCache cache = new InstanceValueRingCache(256);
        long i0 = 10000;
        for (long i = i0; i < i0 + 128; i++) {
            cache.put(newValue(i));
        }

        List<Instance> ix1 = cache.get(i0 + 120, i0 + 130);
        assertEquals(8, ix1.size());
        assertEquals(i0 + 120, ix1.get(0).id());
        assertEquals(i0 + 127, ix1.get(7).id());
    }

    @Test
    public void testGet3() {
        InstanceValueRingCache cache = new InstanceValueRingCache(256);
        long i0 = 10000;
        for (long i = i0; i < i0 + 300; i++) {
            cache.put(newValue(i));
        }

        List<Instance> ix1 = cache.get(i0 + 201, i0 + 290);
        assertEquals(90, ix1.size());
        assertEquals(i0 + 201, ix1.get(0).id());
        assertEquals(i0 + 290, ix1.get(89).id());
    }
}
