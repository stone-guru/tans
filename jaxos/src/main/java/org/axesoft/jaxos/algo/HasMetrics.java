package org.axesoft.jaxos.algo;

/**
 * Indicate the implementation contain metrics which can be formatted to a string
 *
 */
public interface HasMetrics {
    /**
     * Format contained metrics to a whole string
     *
     * @return not null
     */
    String formatMetrics();
}
