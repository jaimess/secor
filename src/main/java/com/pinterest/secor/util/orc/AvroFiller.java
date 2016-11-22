package com.pinterest.secor.util.orc;

import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.bouncycastle.util.Arrays;

public class AvroFiller {
    
    public static void fill(int row, SpecificRecord avroObj, VectorizedRowBatch batch) {
        fill(row, avroObj, batch, null);
    }
    
    public static void fill(int row, SpecificRecord avroObj, VectorizedRowBatch batch, Set<String> skipFields) {
        int colIndex = 0;
        int fieldIndex = 0;
        for (Field field : avroObj.getSchema().getFields()) {
            if (!skipField(field.schema())
                    && (skipFields == null || !skipFields.contains(field.name()))) {
                fillField(row, field.schema(), batch.cols[colIndex], fieldIndex, avroObj);
                colIndex++;
            }
            fieldIndex++;
        }
    }
    
    private static boolean skipField(Schema schema) {
        Type type = schema.getType();
        if (type.equals(Schema.Type.RECORD) || type.equals(Schema.Type.ENUM) || type.equals(Schema.Type.ARRAY)
                || type.equals(Schema.Type.MAP) || type.equals(Schema.Type.FIXED)) 
            return true;
        else if (type.equals(Schema.Type.UNION))
            return skipField(schema.getTypes().get(1));
        else
            return false;
    }
    
    private static void fillField(int row, Schema schema, ColumnVector vector, int fieldIndex, SpecificRecord object) {
        Type type = schema.getType();
        if (type.equals(Schema.Type.BOOLEAN)) {
            object.put(fieldIndex, ((LongColumnVector)vector).vector[row] == 1);
        } else if (type.equals(Schema.Type.INT)) {
            object.put(fieldIndex, (int)(((LongColumnVector)vector).vector[row]));
        } else if (type.equals(Schema.Type.LONG)) {
            object.put(fieldIndex, ((LongColumnVector)vector).vector[row]);            
        } else if (type.equals(Schema.Type.FLOAT)) {
            object.put(fieldIndex, (float)((DoubleColumnVector)vector).vector[row]);
        } else if (type.equals(Schema.Type.DOUBLE)) {
            object.put(fieldIndex, (double)((DoubleColumnVector)vector).vector[row]);
        } else if (type.equals(Schema.Type.BYTES)) {
            BytesColumnVector byteArrayVector = (BytesColumnVector) vector;
            byte[] data = Arrays.copyOfRange(byteArrayVector.vector[row], byteArrayVector.start[row], byteArrayVector.length[row]);
            object.put(fieldIndex, data);
        } else if (type.equals(Schema.Type.STRING)) {
            BytesColumnVector byteArrayVector = (BytesColumnVector) vector;
            if (!vector.isNull[row] && byteArrayVector.length[row] > 0)
                object.put(fieldIndex, new String(((BytesColumnVector) vector).toString(row)));
            else
                object.put(fieldIndex, null);
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
            fillField(row, schema.getTypes().get(1), vector, fieldIndex, object);
        } else {
            throw new UnsupportedOperationException("Cannot convert Avro type " + type);
        }
    }
}
