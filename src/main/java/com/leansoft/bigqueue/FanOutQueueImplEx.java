package com.leansoft.bigqueue;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

/**
 * extension to FanOutQueue to expose write lock of the queue front
 * 
 * @author bulldog
 *
 */
public class FanOutQueueImplEx extends FanOutQueueImpl implements IFanOutQueueEx {

	public FanOutQueueImplEx(String queueDir, String queueName)
			throws IOException {
		super(queueDir, queueName);
	}
	
	public FanOutQueueImplEx(String queueDir, String queueName, int pageSize) throws IOException {
		super(queueDir, queueName, pageSize);
	}


	@Override
	public Lock getQueueFrontWriteLock(String fanoutId) throws IOException {
		return super.getQueueFront(fanoutId).writeLock;
	}

	@Override
	public Lock getInnerArrayReadLock() {
		return super.innerArray.arrayReadLock;
	}

	@Override
	public int getNumberOfBackFiles() {
		return super.innerArray.dataPageFactory.getBackPageFileSet().size();
	}

}
