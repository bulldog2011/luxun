package com.leansoft.bigqueue;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

import com.leansoft.bigqueue.FanOutQueueImplEx.BatchReadResult;

public interface IFanOutQueueEx extends IFanOutQueue  {
	
	Lock getQueueFrontWriteLock(String fanoutId) throws IOException;
	
	Lock getInnerArrayReadLock();
	
	// for testing
	int getNumberOfBackFiles();
	
	// batch dequeue tailored for Luxun batch consuming
	BatchReadResult batchDequeue(String fanoutId, int maxFetchSize) throws IOException;

}
