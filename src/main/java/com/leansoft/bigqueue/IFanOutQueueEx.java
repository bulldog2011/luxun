package com.leansoft.bigqueue;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

public interface IFanOutQueueEx extends IFanOutQueue  {
	
	Lock getQueueFrontWriteLock(String fanoutId) throws IOException;
	
	Lock getInnerArrayReadLock();
	
	// for testing
	int getNumberOfBackFiles();

}
