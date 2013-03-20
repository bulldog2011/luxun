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

package com.leansoft.luxun.utils;

import java.util.regex.Pattern;

import com.leansoft.luxun.common.exception.InvalidTopicException;

/**
 * Topic name validator
 * 
 */
public class TopicNameValidator {

    private static final String illegalChars = "/" + '\u0000' + '\u0001' + "-" + '\u001F' + '\u007F' + "-" + '\u009F' + '\uD800' + "-" + '\uF8FF' + '\uFFF0'
            + "-" + '\uFFFF';
    private static final Pattern p = Pattern.compile("(^\\.{1,2}$)|[" + illegalChars + "]");

    public static void validate(String topic) {
        if (topic.length() == 0) {
            throw new InvalidTopicException("topic name is emtpy");
        }
        if(topic.length() > 255) {
            throw new InvalidTopicException("topic name is too long");
        }
        if (p.matcher(topic).find()) {
            throw new InvalidTopicException("topic name [" + topic + "] is illegal");
        }
    }

}
