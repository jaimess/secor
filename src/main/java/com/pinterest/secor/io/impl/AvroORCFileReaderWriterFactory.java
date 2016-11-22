package com.pinterest.secor.io.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.LzoCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.AvroUtil;
import com.pinterest.secor.util.orc.AvroFiller;
import com.pinterest.secor.util.orc.AvroSchemaConverter;
import com.pinterest.secor.util.orc.VectorColumnFiller;


/**
 * Adapted from
 * com.pinterest.secor.io.impl.ProtobufParquetFileReaderWriterFactory
 * Implementation for reading/writing avro messages to/from Parquet files.
 * 
 * @author jaime sastre (jaime.sastre.s@gmail.com)
 */
public class AvroORCFileReaderWriterFactory implements FileReaderWriterFactory {

    private AvroUtil avroUtil;
    private Set<String> skipFields;

    public AvroORCFileReaderWriterFactory(SecorConfig config) {
        avroUtil = new AvroUtil(config);
        skipFields = resolveSkipFields(config);
    }
    
    private Set<String> resolveSkipFields(SecorConfig config) {
        String skipProperty = config.getORCSkipFields();
        if (skipProperty == null)
            return null;
        else {
            return new HashSet<String>(Arrays.asList(skipProperty.split(",")));
        }
    }



    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvrORCFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroORCFileWriter(logFilePath, codec);
    }
    
    protected class AvrORCFileReader implements FileReader {

        private long offset;
        RecordReader rows; 
        VectorizedRowBatch batch;
        int rowIndex;
        Schema schema;
        
        @SuppressWarnings({ "deprecation", })
        public AvrORCFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            schema = avroUtil.getMessageSchema(logFilePath.getTopic());
            Path path = new Path(logFilePath.getLogFilePath());
            Reader reader = OrcFile.createReader(
                    path,
                    OrcFile.readerOptions(new Configuration(true)));
            offset = logFilePath.getOffset();
            rows = reader.rows();
            batch = reader.getSchema().createRowBatch();
            rows.nextBatch(batch);
        }

        @Override
        public KeyValue next() throws IOException {
            boolean endOfBatch = false;
            if (rowIndex > batch.size)
                endOfBatch = !rows.nextBatch(batch);

            SpecificRecord avroObj;
            try {
                avroObj = (SpecificRecord) Class.forName(schema.getFullName()).newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            if (endOfBatch)
                rows.close();

            AvroFiller.fill(rowIndex, avroObj, batch);
            rowIndex++;
            
            if (avroObj != null) 
            	return new KeyValue(offset++, avroUtil.encodeMessage(avroObj));
            else
            	return null;
        }

        @Override
        public void close() throws IOException {
            rows.close();
        }
    }

    protected class AvroORCFileWriter implements FileWriter {

        private Writer writer;
        private String topic;
        private VectorizedRowBatch batch;
        private Schema schema;
        private int rowIndex;

        public AvroORCFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            topic = logFilePath.getTopic();
            schema = avroUtil.getMessageSchema(topic);
            Path path = new Path(logFilePath.getLogFilePath());
            Configuration conf = new Configuration();
            TypeDescription typeDefinition = AvroSchemaConverter.generateTypeDefinition(schema, skipFields);
            writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf)
                    .compress(resolveCompression(codec))
                    .setSchema(typeDefinition ));
            batch = typeDefinition.createRowBatch();
            
        }

        @Override
        public long getLength() throws IOException {
            return writer.getRawDataSize();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            rowIndex = batch.size++;
            SpecificRecord data = avroUtil.decodeMessage(topic, keyValue.getValue());

            VectorColumnFiller.fillRow(rowIndex, schema, batch, data, skipFields);
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        @Override
        public void close() throws IOException {
            writer.addRowBatch(batch);
            writer.close();
        }
    }
    
    private CompressionKind resolveCompression(CompressionCodec codec) {
        if (codec instanceof Lz4Codec)
            return CompressionKind.LZ4;
        else if (codec instanceof SnappyCodec) 
            return CompressionKind.SNAPPY;
        else if (codec instanceof SnappyCodec) 
            return CompressionKind.SNAPPY;
        else if (codec instanceof LzoCodec) 
            return CompressionKind.LZO;
        else
            return CompressionKind.NONE;
    }
}
