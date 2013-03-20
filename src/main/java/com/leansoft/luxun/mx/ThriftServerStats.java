/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.leansoft.luxun.mx;

import com.leansoft.luxun.api.generated.ConsumeRequest;
import com.leansoft.luxun.api.generated.ProduceRequest;

/**
 * @author bulldog
 * 
 */
public class ThriftServerStats implements ThriftServerStatsMBean, IMBeanName {

    final long monitorDurationNs;

    //=======================================
    final SnapshotStats produceTimeStats;

    final SnapshotStats consumeTimeStats;

    final SnapshotStats produceBytesStats;

    final SnapshotStats consumeBytesStats;

    //=======================================
    public ThriftServerStats(long monitorDurationNs) {
        this.monitorDurationNs = monitorDurationNs;
        produceTimeStats = new SnapshotStats(monitorDurationNs);
        consumeTimeStats = new SnapshotStats(monitorDurationNs);
        produceBytesStats = new SnapshotStats(monitorDurationNs);
        consumeBytesStats = new SnapshotStats(monitorDurationNs);
    }

    public void recordBytesRead(int bytes) {
        produceBytesStats.recordRequestMetric(bytes);
    }

    public void recordRequest(Class<?> requestClass, long durationNs) {
    	if (requestClass == ProduceRequest.class) {
            produceTimeStats.recordRequestMetric(durationNs);
    	} else if (requestClass == ConsumeRequest.class) {
    		consumeTimeStats.recordRequestMetric(durationNs);
    	}
    }

    public void recordBytesWritten(int bytes) {
        consumeBytesStats.recordRequestMetric(bytes);
    }

    public double getProduceRequestsPerSecond() {
        return produceTimeStats.getRequestsPerSecond();
    }

    public double getConsumeRequestsPerSecond() {
        return consumeTimeStats.getRequestsPerSecond();
    }

    public double getAvgProduceRequestMs() {
        return produceTimeStats.getAvgMetric() / (1000.0 * 1000.0);
    }

    public double getMaxProduceRequestMs() {
        return produceTimeStats.getMaxMetric() / (1000.0 * 1000.0);
    }

    public double getAvgConsumeRequestMs() {
        return consumeTimeStats.getAvgMetric() / (1000.0 * 1000.0);
    }

    public double getMaxConsumeRequestMs() {
        return consumeTimeStats.getMaxMetric() / (1000.0 * 1000.0);
    }

    public double getBytesReadPerSecond() {
        return produceBytesStats.getAvgMetric();
    }

    public double getBytesWrittenPerSecond() {
        return consumeBytesStats.getAvgMetric();
    }

    public long getNumConsumeRequests() {
        return consumeTimeStats.getNumRequests();
    }

    public long getNumProduceRequests() {
        return produceTimeStats.getNumRequests();
    }

    public long getTotalBytesRead() {
        return produceBytesStats.getTotalMetric();
    }

    public long getTotalBytesWritten() {
        return consumeBytesStats.getTotalMetric();
    }

    public long getTotalConsumeRequestMs() {
        return consumeTimeStats.getTotalMetric();
    }

    public long getTotalProduceRequestMs() {
        return produceTimeStats.getTotalMetric();
    }

    public String getMbeanName() {
        return "luxun:type=luxun.ThriftServerStats";
    }
}
