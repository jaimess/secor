package com.pinterest.secor.io.impl;

import java.io.IOException;










import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.AvroUtil;

/**
 * Adapted from
 * com.pinterest.secor.io.impl.ProtobufParquetFileReaderWriterFactory
 * Implementation for reading/writing avro messages to/from Parquet files.
 * 
 * @author jaime sastre (jaime.sastre.s@gmail.com)
 */
public class AvroParquetFileReaderWriterFactory implements FileReaderWriterFactory {

    private AvroUtil avroUtil;

    public AvroParquetFileReaderWriterFactory(SecorConfig config) {
        avroUtil = new AvroUtil(config);
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroParquetFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroParquetFileWriter(logFilePath, codec);
    }

    protected class AvroParquetFileReader implements FileReader {

        private ParquetReader<SpecificRecord> reader;
        private long offset;

        @SuppressWarnings({ "deprecation", "unchecked" })
        public AvroParquetFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            reader = AvroParquetReader.builder(path).build();
            offset = logFilePath.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            SpecificRecord msg = reader.read();

            if (msg != null) 
            	return new KeyValue(offset++, avroUtil.encodeMessage(msg));
            else
            	return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    protected class AvroParquetFileWriter implements FileWriter {

        @SuppressWarnings("rawtypes")
        private ParquetWriter writer;
        private String topic;

        public AvroParquetFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            CompressionCodecName codecName = CompressionCodecName.fromCompressionCodec(codec != null ? codec.getClass() : null);
            topic = logFilePath.getTopic();
            Schema schema = avroUtil.getMessageSchema(topic);
            writer = AvroParquetWriter.builder(path).withCompressionCodec(codecName).withSchema(schema).build();
        }

        @Override
        public long getLength() throws IOException {
            return writer.getDataSize();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void write(KeyValue keyValue) throws IOException {
            Object message;
            try {
                message = avroUtil.decodeMessage(topic, keyValue.getValue());
                writer.write(message);
            } catch (Exception e) {
                throw new IOException("cannot write message", e);
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
