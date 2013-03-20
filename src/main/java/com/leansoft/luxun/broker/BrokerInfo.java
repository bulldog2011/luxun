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

package com.leansoft.luxun.broker;


import java.io.Closeable;
import java.util.List;


/**
 * 
 * @author bulldog
 * 
 */
public interface BrokerInfo extends Closeable {
    
    /**
     * Return a list of broker ids
     * 
     * @return broker id list
     */
    List<Integer> getBrokerIdList();

    /**
     * Get broker by id
     * 
     * @param brokerId the broker for which the info is to be returned
     * @return host and port of brokerId
     */
    Broker getBrokerInfo(int brokerId);

    /**
     * Cleanup
     */
    void close();
}
