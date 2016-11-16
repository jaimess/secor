package com.pinterest.secor.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
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
public class AvroUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AvroUtil.class);

    private boolean allTopics;
    private Map<String, Schema> messageSchemaByTopic = new HashMap<String, Schema>();
    private Schema messageSchemaForAll;

    /**
     * Creates new instance of {@link AvroUtil}
     * 
     * @param config
     *            Secor configuration instance
     * @throws RuntimeException
     *             when configuration option
     *             <code>secor.avro.message.class</code> is invalid.
     */
    @SuppressWarnings({ "unchecked" })
    public AvroUtil(SecorConfig config) {
        Map<String, String> messageSchemaPerTopic = config.getAvroMessageClassPerTopic();
        
        for (Entry<String, String> entry : messageSchemaPerTopic.entrySet()) {
            try {
                String topic = entry.getKey();
                
                Class<? extends SpecificRecord> clazz = (Class<? extends SpecificRecord>) Class.forName(entry.getValue());
                Schema messageSchema = ReflectData.get().getSchema(clazz);;

                allTopics = "*".equals(topic);

                if (allTopics) {
                    messageSchemaForAll = messageSchema;
                    LOG.info("Using avro message class: {} for all Kafka topics", messageSchema.getName());
                } else {
                    messageSchemaByTopic.put(topic, messageSchema);
                    LOG.info("Using avro message class: {} for Kafka topic: {}", messageSchema.getName(), topic);
                }
            } catch (ClassNotFoundException e) {
                LOG.error("Unable to load avro message class " + entry.getValue() , e);
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
    	SpecificDatumReader reader = new SpecificDatumReader(this.getMessageSchema(topic));
    	try {
			return (SpecificRecord) reader.read(null, decoder);
		} catch (IOException e) {
            LOG.error("cannot deserialize object", e);
            return null;
		}
    }

    public byte[] encodeMessage(SpecificRecord object) {
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
    	BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    	DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<SpecificRecord>(object.getSchema());

    	try {
			writer.write(object, encoder);
			encoder.flush();
			out.close();
    	} catch (IOException e) {
            LOG.error("cannot serialize " + object, e);	
    	}
    	return out.toByteArray();
    }
}