package com.pinterest.secor.util.orc;

import static org.apache.orc.TypeDescription.createBinary;
import static org.apache.orc.TypeDescription.createBoolean;
import static org.apache.orc.TypeDescription.createDouble;
import static org.apache.orc.TypeDescription.createFloat;
import static org.apache.orc.TypeDescription.createInt;
import static org.apache.orc.TypeDescription.createLong;
import static org.apache.orc.TypeDescription.createString;
import static org.apache.orc.TypeDescription.createTimestamp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;


/**
 * <p>
 * Converts an Avro schema into a Parquet schema. See package documentation for
 * details of the mapping.
 * </p>
 */
public class AvroSchemaConverter {
    private static final Map<Schema, TypeDescription> cache = new HashMap<Schema, TypeDescription>();
    
    public static TypeDescription generateTypeDefinition(Schema avroSch, Set<String> skipFields) {
        if (cache.containsKey(avroSch))
            return cache.get(avroSch);
        
        TypeDescription schema = TypeDescription.createStruct();
        for (Field field :avroSch.getFields()) {
            if (skipFields == null || !skipFields.contains(field.name()))
            schema = convertField(field.name(), field.schema(), schema);
        }
        cache.put(avroSch, schema);
        return schema;
    }
    
    public static TypeDescription convertField(String fieldName, Schema schema, TypeDescription struct) {
        Schema.Type type = schema.getType();
        if (type.equals(Schema.Type.BOOLEAN)) {
            struct.addField(fieldName, createBoolean());
        } else if (type.equals(Schema.Type.INT)) {
            struct.addField(fieldName, createInt());
        } else if (type.equals(Schema.Type.LONG)) {
            if (schema.getLogicalType() != null && schema.getLogicalType().equals(LogicalTypes.timestampMillis()))
                struct.addField(fieldName, createTimestamp());
            else
                struct.addField(fieldName, createLong());
        } else if (type.equals(Schema.Type.FLOAT)) {
            struct.addField(fieldName, createFloat());
        } else if (type.equals(Schema.Type.DOUBLE)) {
            struct.addField(fieldName, createDouble());
        } else if (type.equals(Schema.Type.BYTES)) {
            struct.addField(fieldName, createBinary());
        } else if (type.equals(Schema.Type.STRING)) {
            struct.addField(fieldName, createString());
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
            //asuming an optional field, null + valid type
            convertField(fieldName, schema.getTypes().get(1), struct);
        } else {
            throw new UnsupportedOperationException("Cannot convert Avro type " + type);
        }
        return struct;
    }

    public static TypeDescription generateTypeDefinition(Schema schema) {
        return generateTypeDefinition(schema, null);
    }
    
}