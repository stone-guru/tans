package org.axesoft.jaxos.algo;

import org.axesoft.jaxos.JaxosService;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author bison
 * @sine 2019/11/28.
 */
public class ProposerTest {
    @Test
    public void testBallotId1() throws Exception {
        JaxosService.MessageIdHolder holder = new JaxosService.MessageIdHolder(7);
        long i1 = holder.nextIdOf(2);
        //System.out.println(String.format("%X", i1));

        long i2 = holder.nextIdOf(2);
        //System.out.println(String.format("%X", i2));

        assertEquals(i2, i1 + 1);
    }

    @Ignore
    public void testBallotId2() throws Exception {
        JaxosService.MessageIdHolder holder = new JaxosService.MessageIdHolder(7);
        for(int i = 0;i < Integer.MAX_VALUE; i++){
            holder.nextIdOf(3);
        }
        long i1 = holder.nextIdOf(3);
        //System.out.println(String.format("%X", i1));

        for(int i = 0;i < Integer.MAX_VALUE; i++){
            holder.nextIdOf(3);
        }
        long i2 = holder.nextIdOf(3);
        //System.out.println(String.format("%X", i2));

        long i3 = holder.nextIdOf(3);
        //System.out.println(String.format("%X", i3));
        assertEquals(7L << 32, i3);
    }
}
