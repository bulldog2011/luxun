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

package com.leansoft.luxun.producer;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the data to be sent using the Producer send API
 * 
 * @author bulldog
 * 
 */
public class ProducerData<V> {

    /** the topic under which the message is to be published */
    private String topic;

    /** variable length data to be published as Luxun messages under topic */
    private List<V> data;

    public ProducerData(String topic, List<V> data) {
        super();
        this.topic = topic;
        this.data = data;
    }
    
    public ProducerData(String topic, V data) {
    	this.topic = topic;
    	getData().add(data);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<V> getData() {
        if (data == null) {
            data = new ArrayList<V>();
        }
        return data;
    }

    public void setData(List<V> data) {
        this.data = data;
    }
}
