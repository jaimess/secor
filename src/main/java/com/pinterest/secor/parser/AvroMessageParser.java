/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.AvroUtil;

/**
 * Thrift message parser extracts date partitions from thrift messages.
 *
 * @author jaime sastre (jaime.sastre.s@gmail.com)
 */
public class AvroMessageParser extends TimestampedMessageParser {
    SpecificDatumReader<? extends SpecificRecord> mReader;
    private final AvroUtil mAvroUtil;
    private final Map<String, AvroField> mFieldByTopic;
    
    private class AvroField {
        private final int mFieldIndex;
        
        public AvroField(int mFieldIndex, String mFieldType) {
            this.mFieldIndex = mFieldIndex;
        }        
    }

    public AvroMessageParser(SecorConfig config)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        super(config);
        mAvroUtil = new AvroUtil(config);
        mFieldByTopic = new HashMap<String, AvroField>();
    }

    @Override
    public long extractTimestampMillis(final Message message)  {
        AvroField avroField = getAvroField(message.getTopic());
        SpecificRecord data = mAvroUtil.decodeMessage(message.getTopic(), message.getPayload());
        Long timestamp = (Long) data.get(avroField.mFieldIndex);
        return toMillis(timestamp);
    }
    
    private AvroField getAvroField(String topic) {
        AvroField AvroField = mFieldByTopic.get(topic);
        if (AvroField != null)
            return AvroField;
        else {
            AvroField = new AvroField(mAvroUtil.getTimestampIndex(topic),
                    mAvroUtil.getTimestampType(topic));
            mFieldByTopic.put(topic, AvroField);
            return AvroField;
        }
    }
}
