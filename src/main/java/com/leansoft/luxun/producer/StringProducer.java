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

import com.leansoft.luxun.serializer.Encoder;
import com.leansoft.luxun.serializer.StringEncoder;

/**
 * UTF-8 String Producer
 * 
 * @author bulldog
 * 
 */
public class StringProducer extends Producer<String> implements IStringProducer {

    /**
     * create a string producer with given config
     * 
     * @param config producer config
     */
    public StringProducer(ProducerConfig config) {
        super(config, null, true, null);
    }


    @Override
    public Encoder<String> getEncoder() {
        return new StringEncoder();
    }

}
