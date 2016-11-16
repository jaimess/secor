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
package com.pinterest.secor.io.impl;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.io.Files;
import com.pinterest.secor.avro.UnitTestMessage;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.ReflectionUtil;

@RunWith(PowerMockRunner.class)
public class AvroParquetFileReaderWriterFactoryTest extends TestCase {

    private SecorConfig config;

    @Override
    public void setUp() throws Exception {
        config = Mockito.mock(SecorConfig.class);
    }

    @Test
    public void testThriftParquetReadWriteRoundTrip() throws Exception {
        Map<String, String> classPerTopic = new HashMap<String, String>();
        classPerTopic.put("test-pb-topic", UnitTestMessage.class.getName());
        Mockito.when(config.getAvroMessageClassPerTopic()).thenReturn(classPerTopic);
        Mockito.when(config.getFileReaderWriterFactory())
                .thenReturn(AvroParquetFileReaderWriterFactory.class.getName());

        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(), "test-pb-topic",
                new String[] { "part-1" }, 0, 1, 23232, ".log");

        FileWriter fileWriter = ReflectionUtil.createFileWriter(config.getFileReaderWriterFactory(), tempLogFilePath,
                null, config);

        UnitTestMessage msg1 = new UnitTestMessage();
        msg1.setRequiredField("abc");
        msg1.setTimestamp(1467176315L);
        UnitTestMessage msg2 = new UnitTestMessage();
        msg2.setRequiredField("XYZ");
        msg2.setTimestamp(1467176344L);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<UnitTestMessage> writer = new SpecificDatumWriter<UnitTestMessage>(ReflectData.get().getSchema(UnitTestMessage.class));
        
        writer.write(msg1, encoder);
        encoder.flush();
        out.close();
        byte[] serializedBytes = out.toByteArray();

        out = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(msg2, encoder);
        encoder.flush();
        out.close();
        byte[] serializedBytes2 = out.toByteArray();

        KeyValue kv1 = new KeyValue(23232, serializedBytes);
        KeyValue kv2 = new KeyValue(23233, serializedBytes2);
        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();

        FileReader fileReader = ReflectionUtil.createFileReader(config.getFileReaderWriterFactory(), tempLogFilePath,
                null, config);
        
        KeyValue kvout = fileReader.next();
        Decoder decoder = DecoderFactory.get().binaryDecoder(kvout.getValue(), null);
        SpecificDatumReader<UnitTestMessage> reader = new SpecificDatumReader<UnitTestMessage>(ReflectData.get().getSchema(UnitTestMessage.class));
        UnitTestMessage actual = reader.read(null, decoder);

        assertEquals(kv1.getOffset(), kvout.getOffset());
        assertArrayEquals(kv1.getValue(), kvout.getValue());
        assertEquals(msg1.getRequiredField().toString(), actual.getRequiredField().toString());

        kvout = fileReader.next();
        decoder = DecoderFactory.get().binaryDecoder(kvout.getValue(), null);
        actual = reader.read(null, decoder);
        
        assertEquals(kv2.getOffset(), kvout.getOffset());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
        assertEquals(msg2.getRequiredField().toString(), actual.getRequiredField().toString());
    }
}