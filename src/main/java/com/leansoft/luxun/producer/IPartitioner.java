package com.leansoft.luxun.producer;

/**
 * Send messages to specific broker(server)
 * 
 * @author bulldog
 *
 */
public interface IPartitioner<T> {

	/**
	 * Uses the key to calculate a broker id for routing the data to the appropriate broker.
	 * 
	 * @param key partition key
	 * @param numBrokers number of brokers
	 * @return broker id
	 */
	int partition(T key, int numBrokers);
	
}
