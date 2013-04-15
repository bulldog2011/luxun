package com.leansoft.bigqueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

import com.leansoft.luxun.common.annotations.NotThreadSafe;

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
	
	// batch dequeue tailored for Luxun batch consuming
	@NotThreadSafe
	public BatchReadResult batchDequeue(String fanoutId, int maxFetchSize) throws IOException {
		
			QueueFront qf = this.getQueueFront(fanoutId);
			
			int totalFetchedSize = 0;
			List<ByteBuffer> bufferList = new ArrayList<ByteBuffer>();
			while (qf.index.get() != innerArray.arrayHeadIndex.get() && totalFetchedSize < maxFetchSize) {
				
				int length = innerArray.getItemLength(qf.index.get());
				if (totalFetchedSize + length > maxFetchSize) {
					break;
				}
			
				byte[] data = null;
				try {
					data = innerArray.get(qf.index.get());
				} catch (IndexOutOfBoundsException ex) {
					// maybe the back array has just been truncated to limit size
					qf.index.set(innerArray.arrayTailIndex.get());
					data = innerArray.get(qf.index.get());
				}
					
				bufferList.add(ByteBuffer.wrap(data));
				totalFetchedSize += length;
				qf.index.incrementAndGet();
				if (qf.index.longValue() < 0) {
					qf.index.set(0); //wrap
				}
			}
			
			if (bufferList.size() > 0) { // commit consuming
				qf.persistIndex();
			}
			
			BatchReadResult result = new BatchReadResult();
			result.bufferList = bufferList;
			result.totalFetchedSize = totalFetchedSize;
			
			return result;
	}
	
	public static class BatchReadResult {
		public List<ByteBuffer> bufferList;
		public int totalFetchedSize;
	}

}
