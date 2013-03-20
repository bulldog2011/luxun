package com.leansoft.luxun.mx;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import com.leansoft.luxun.log.Log;

public class LogStats implements LogStatsMBean, IMBeanName {
	
	final Log log;
	
    private final AtomicLong numCumulatedMessages = new AtomicLong(0);

    private String mbeanName;

	public LogStats(Log log) {
		this.log = log;
	}

	@Override
	public String getMbeanName() {
		return mbeanName;
	}
	
    public void setMbeanName(String mbeanName) {
        this.mbeanName = mbeanName;
    }

	@Override
	public String getName() {
		return this.log.topic;
	}
	
    public void incrementAppendedMessageCount() {
        numCumulatedMessages.incrementAndGet();
    }

	@Override
	public String getLastFlushedTime() {
		long time = log.getLastFlushedTime();
		if (time == 0) return "--";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(new Date(time));
	}

	@Override
	public long getAppendedMessageNumberSinceLatestStart() {
		return this.numCumulatedMessages.get();
	}

	@Override
	public long getFrontIndex() {
		return log.getFrontIndex();
	}

	@Override
	public long getLastIndex() {
		// rearIndex is the next to be appended index
		// lastIndex is the last appended index
		// lastIndex == rearIndex - 1
		long lastIndex = log.getRearIndex();
		if (lastIndex == 0L) {
			lastIndex = Long.MAX_VALUE;
		} else {
			lastIndex--;
		}
		return lastIndex;
	}

	@Override
	public boolean isEmpty() {
		return this.log.isEmpty();
	}

	@Override
	public long getTotalMessageNumber() {
		return this.log.getSize();
	}

	@Override
	public long getBackFileSize() {
		try {
			return this.log.getBackFileSize();
		} catch (IOException e) {
			return -1L;
		}
	}

}
