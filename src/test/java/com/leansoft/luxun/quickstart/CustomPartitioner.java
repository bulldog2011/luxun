package com.leansoft.luxun.quickstart;

import com.leansoft.luxun.producer.IPartitioner;

public class CustomPartitioner implements IPartitioner<String> {
	@Override
	public int partition(String key, int numBrokers) {
		return (key.length() % numBrokers);
	}
}
