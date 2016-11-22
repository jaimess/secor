package com.pinterest.secor.util.orc;

import java.nio.charset.Charset;
import java.util.Set;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class VectorColumnFiller {
    private final static Charset UTF8 = Charset.forName("UTF8");
    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
    
    public static void fillRow(int row, Schema schema, VectorizedRowBatch batch, SpecificRecord data) {
        fillRow(row, schema, batch, data);
    }
    
    public static void fillRow(int row, Schema schema, VectorizedRowBatch batch, SpecificRecord data, Set<String> skipFields) {
        int fieldIndex = 0;
        int colIndex = 0;
        for (Field field : schema.getFields()) {
            if (!skipMappedField(field.schema()) && 
                    (skipFields == null || !skipFields.contains(field.name()))) {
                fillField(row, field.schema(), batch.cols[colIndex], data.get(fieldIndex));
                colIndex++;
            }
            fieldIndex++;
        }
    }

    private static boolean skipMappedField(Schema schema) {
        Type type = schema.getType();
        if (type.equals(Schema.Type.RECORD) || type.equals(Schema.Type.ENUM) || type.equals(Schema.Type.ARRAY)
                || type.equals(Schema.Type.MAP) || type.equals(Schema.Type.FIXED)) 
            return true;
        else if (type.equals(Schema.Type.UNION))
            return skipMappedField(schema.getTypes().get(1));
        else
            return false;
    }

    private static void fillField(int row, Schema schema, ColumnVector vector, Object object) {
        Type type = schema.getType();
        if (type.equals(Schema.Type.BOOLEAN)) {
            fill(row, (LongColumnVector) vector, ((Boolean) object) ? 1L : 0L);
        } else if (type.equals(Schema.Type.INT)) {
            fill(row, (LongColumnVector) vector, ((Integer) object));
        } else if (type.equals(Schema.Type.LONG)) {
            if (schema.getLogicalType() != null && schema.getLogicalType().equals(LogicalTypes.timestampMillis()))
                fill(row, (TimestampColumnVector) vector, ((Long) object));
            else
                fill(row, (LongColumnVector) vector, ((Long) object));
        } else if (type.equals(Schema.Type.FLOAT)) {
            fill(row, (DoubleColumnVector) vector, ((Float) object));
        } else if (type.equals(Schema.Type.DOUBLE)) {
            fill(row, (DoubleColumnVector) vector, ((Double) object));
        } else if (type.equals(Schema.Type.BYTES)) {
            fill(row, (BytesColumnVector) vector, ((byte[]) object));
        } else if (type.equals(Schema.Type.STRING)) {
            if (object != null)
                fill(row, (BytesColumnVector) vector, (((Utf8) object).getBytes()));
            else {
                ((BytesColumnVector) vector).setRef(row, EMPTY_BYTE_ARRAY, 0, 0);
                vector.isNull[row] = true;
            }
        } else if (type.equals(Schema.Type.RECORD)) {
            // skipping by now
        } else if (type.equals(Schema.Type.ENUM)) {
            // skipping by now
        } else if (type.equals(Schema.Type.ARRAY)) {
            // skipping by now
        } else if (type.equals(Schema.Type.MAP)) {
            // skipping by now
        } else if (type.equals(Schema.Type.FIXED)) {
            // skipping by now
        } else if (type.equals(Schema.Type.UNION)) {
            fillField(row, schema.getTypes().get(1), vector, object);
        } else {
            throw new UnsupportedOperationException("Cannot convert Avro type " + type);
        }
    }

    private static void fill(int row, BytesColumnVector vector, byte[] field) {
        if (field != null)
            vector.setRef(row, field, 0, field.length);
    }

    private static void fill(int row, DoubleColumnVector vector, Double field) {
        if (field != null)
            vector.vector[row] = field;
    }

    private static void fill(int row, DoubleColumnVector vector, Float field) {
        if (field != null)
            vector.vector[row] = field;
    }

    private static void fill(int row, TimestampColumnVector vector, Long field) {
        if (field != null)
            vector.time[row] = field;
    }

    private static void fill(int row, LongColumnVector vector, Integer field) {
        if (field != null)
            vector.vector[row] = field;
    }

    private static void fill(int row, LongColumnVector vector, Long field) {
        if (field != null)
            vector.vector[row] = field;
    }

}
