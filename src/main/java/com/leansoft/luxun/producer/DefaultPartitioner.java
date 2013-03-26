package com.leansoft.luxun.producer;

import java.util.Random;

public class DefaultPartitioner<T> implements IPartitioner<T> {

	private final Random random = new Random();
	
	@Override
	public int partition(T key, int numBrokers) {
		if (key == null) {
			return random.nextInt(numBrokers);
		}
		return Math.abs(key.hashCode()) % numBrokers;
	}

}
