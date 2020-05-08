package org.axesoft.jaxos.algo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProposalNumberHolderTest {
    Proposer.ProposalNumHolder holder = new Proposer.ProposalNumHolder(3, 16);

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorFail(){
        new Proposer.ProposalNumHolder(9, 8);
    }

    @Test
    public void testProposal0() {
        assertEquals(3, holder.getProposal0());
    }

    @Test
    public void testNextProposal1(){
        //50 = 3 * 16 + 2, next one should be 3 * 16 + 3, where 3 is own server id
        assertEquals(51, holder.proposalGreatThan(50));
        //52 = 3 * 16 + 4, next should be 4 * 16 + 3 = 67
        assertEquals(67, holder.proposalGreatThan(52));
    }
}
