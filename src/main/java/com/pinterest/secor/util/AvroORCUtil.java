package com.pinterest.secor.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.SecorConfig;

/**
 * Adapted from ProtobufUtil Various utilities for working with avro encoded
 * messages. This utility will look for avro class in the configuration. It
 * can be either per Kafka topic configuration, for example:
 * 
 * <code>secor.avro.message.class.&lt;topic&gt;=&lt;avro class name&gt;</code>
 * 
 * or, it can be global configuration for all topics (in case all the topics
 * transfer the same message type):
 * 
 * <code>secor.avro.message.class.*=&lt;avro class name&gt;</code>
 * 
 * @author jaime sastre (jaime sastre.s@gmail.com)
 */
public class AvroORCUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AvroORCUtil.class);

    private boolean allTopics;
    private Map<String, Schema> messageSchemaByTopic = new HashMap<String, Schema>();
    private Schema messageSchemaForAll;
    private Short timestampIndexForAll;
    private Map<String, Integer> timestampIndexByTopic = new HashMap<String, Integer>();
    private boolean sameTimestampIndexForallTopics;
    private String timestampTypeForAll;
    private Map<String, String> timestampTypeByTopic = new HashMap<String, String>();
    private boolean sameTimestampTypeForallTopics;   


    /**
     * Creates new instance of {@link AvroORCUtil}
     * 
     * @param config
     *            Secor configuration instance
     * @throws RuntimeException
     *             when configuration option
     *             <code>secor.avro.message.class</code> is invalid.
     */
    @SuppressWarnings({ "unchecked" })
    public AvroORCUtil(SecorConfig config) {        
        resolveMessageClass(config);
        resolveTimestampIndex(config);
        resolveTimestampType(config);
    }
    
    private void resolveMessageClass(SecorConfig config) {
        Map<String, String> messageClassPerTopic = config.getAvroMessageClassPerTopic();
        
        for (Entry<String, String> entry : messageClassPerTopic.entrySet()) {
            try {
                String topic = entry.getKey();
                Schema messageSchema = ReflectData.get().getSchema(Class.forName(entry.getValue()));

                allTopics = "*".equals(topic);

                if (allTopics) {
                    messageSchemaForAll = messageSchema;
                    LOG.info("Using avro message class: {} for all Kafka topics", entry.getValue());
                } else {
                    messageSchemaByTopic.put(topic, messageSchema);
                    LOG.info("Using avro message class: {} for Kafka topic: {}", entry.getValue(), topic);
                }
            } catch (ClassNotFoundException e) {
                LOG.error("Unable to load avro message class", e);
            }
        }
    }
    /**
     * Returns configured avro message class for the given Kafka topic
     * 
     * @param topic
     *            Kafka topic
     * @return avro message class used by this utility instance, or
     *         <code>null</code> in case valid class couldn't be found in the
     *         configuration.
     */
    public Schema getMessageSchema(String topic) {
        return allTopics ? messageSchemaForAll : messageSchemaByTopic.get(topic);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public SpecificRecord decodeMessage(String topic, byte[] payload) {
    	Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
    	
    	DatumReader reader = SpecificData.get().createDatumReader(this.getMessageSchema(topic));
    	try {
 			return (SpecificRecord) reader.read(null, decoder);
		} catch (IOException e) {
            LOG.error("cannot deserialize object", e);
            return null;
		}
    }

    public byte[] encodeMessage(Object object) {
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
    	BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    	DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<SpecificRecord>(((SpecificRecord)object).getSchema());

    	try {
			writer.write((SpecificRecord) object, encoder);
			encoder.flush();
			out.close();
    	} catch (IOException e) {
            LOG.error("cannot serialize " + object, e);	
    	}
    	return out.toByteArray();
    }
    
    private void resolveTimestampIndex(SecorConfig config) {
        Map<String, Integer> tstampIndexPerTopic = config.getMessageTimestampIdPerTopic();
            
        if (tstampIndexPerTopic.isEmpty() && config.getMessageTimestampId() > 0) {
            sameTimestampIndexForallTopics = true;
            timestampIndexForAll = (short) config.getMessageTimestampId();
            return;
        }
        
        for (Entry<String, Integer> entry : tstampIndexPerTopic.entrySet()) {
            String topic = entry.getKey();
            sameTimestampIndexForallTopics = "*".equals(topic);
    
            if (sameTimestampIndexForallTopics) 
                timestampIndexForAll = entry.getValue().shortValue();
            else 
                timestampIndexByTopic.put(topic, entry.getValue());
        }
    }
    
    private void resolveTimestampType(SecorConfig config) {
        Map<String, String> tstampTypePerTopic = config.getMessageTimestampTypePerTopic();
        
        if (tstampTypePerTopic.isEmpty() && StringUtils.isNotEmpty(config.getMessageTimestampType())) {
            sameTimestampTypeForallTopics = true;
            timestampTypeForAll = config.getMessageTimestampType();
        }
        
        for (Entry<String, String> entry : tstampTypePerTopic.entrySet()) {
            String topic = entry.getKey();
            sameTimestampTypeForallTopics = "*".equals(topic);
    
            if (sameTimestampTypeForallTopics)
                timestampTypeForAll = entry.getValue();
            else
                timestampTypeByTopic.put(topic, entry.getValue());
        }
    }
    
    public Integer getTimestampIndex(String topic) {
        return this.sameTimestampIndexForallTopics ? timestampIndexForAll : timestampIndexByTopic.get(topic);
    }
    
    public String getTimestampType(String topic) {
        return this.sameTimestampTypeForallTopics ? timestampTypeForAll : timestampTypeByTopic.get(topic);
    }        
}