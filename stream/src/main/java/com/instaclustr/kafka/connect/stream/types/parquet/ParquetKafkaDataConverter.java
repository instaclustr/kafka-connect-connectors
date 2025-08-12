package com.instaclustr.kafka.connect.stream.types.parquet;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;


public class ParquetKafkaDataConverter {

    final private ParquetKafkaTypeConverter typeConverter;

    public static ParquetKafkaDataConverter newConverter() {
        ParquetKafkaTypeConverter typeConverter = new ParquetKafkaTypeConverter();
        return new ParquetKafkaDataConverter(typeConverter);
    }

    public ParquetKafkaDataConverter(ParquetKafkaTypeConverter typeConverter) {
        this.typeConverter = typeConverter;
    }

    public Struct convert(SimpleGroup group) {
        assert group.getType() instanceof MessageType : "Unexpected Parquet root schema type: " + group.getType().getClass();
        Schema kafkaSchema = ((MessageType) group.getType()).convertWith(typeConverter);
        return convert(kafkaSchema, group);
    }

    public Struct convert(Schema kafkaSchema, SimpleGroup group) {
        // Parquet row, aka SimpleGroup:
        // - Its schema matches the present fields in the row
        // - Its fields are always lists, where empty list means null optional value, singleton list for present optional, required, or repeated-once value, multi-element list for repeated value
        // Kafka row, aka Struct:
        // - Its schema matches the present fields in the row, same as Parquet
        // - Its fields are NOT always lists: null means null optional value, non-collection object for present optional or required value, a list for repeated value
        // This difference is due to type systems: List is reserved for Kafka's ARRAY type
        Struct result = new Struct(kafkaSchema);
        int fieldIndex = 0;
        for (Type field : group.getType().getFields()) {
            final String name = field.getName();
            final int reps = group.getFieldRepetitionCount(fieldIndex);
            switch (field.getRepetition()) {
                case REQUIRED: {
                    assert reps == 1 : "Required value should appear once, but found: " + reps;
                    Object kafkaValue = convertFieldSingleton(kafkaSchema, group, field, fieldIndex);
                    result.put(name, kafkaValue);
                }
                break;

                case OPTIONAL: {
                    if (reps <= 0) {
                        result.put(name, null);
                    } else {
                        assert reps == 1 : "Optional value should be present no more than once, but found: " + reps;
                        Object kafkaValue = convertFieldSingleton(kafkaSchema, group, field, fieldIndex);
                        result.put(name, kafkaValue);
                    }
                }
                break;

                case REPEATED: {
                    List<Object> values = convertFieldRepeated(kafkaSchema, group, field, reps, fieldIndex);
                    result.put(name, values);
                }
                break;

                default:
                    throw new ConverterError("Unexpected repetition: " + field.getRepetition());
            }
            ++fieldIndex;
        }
        return result;
    }

    /**
     * Convert a Parquet Group's Field that is repeated.  Returns a List of values.
     */
    private List<Object> convertFieldRepeated(final Schema parentSchema, final SimpleGroup group, final Type field, final int reps, final int fieldIndex) {
        List<Object> values = new ArrayList<>(reps);
        if (field instanceof GroupType) {
            // For repeated values as Groups, recurs into {@link #convert(Schema, SimpleGroup) convert} method.
            for (int r = 0; r < reps; r++) {
                Object value = group.getObject(fieldIndex, r);
                assert value instanceof SimpleGroup : "Each repeated nested field should be SimpleGroup";
                Schema kafkaFieldSchema = parentSchema.fields().get(fieldIndex).schema().valueSchema();
                values.add(convert(kafkaFieldSchema, (SimpleGroup) value));
            }
        } else {
            // Primitive
            assert field instanceof PrimitiveType : "Unexpected Parquet field type: " + field.getClass();
            switch (((PrimitiveType) field).getPrimitiveTypeName()) {
                case BINARY:
                    LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();
                    if (LogicalTypeAnnotation.stringType().equals(logicalType)) {
                        for (int r = 0; r < reps; r++) {
                            Object value = group.getObject(fieldIndex, r);
                            assert value instanceof Binary : "Unexpected Parquet value type for UTF-8 String: " + value.getClass();
                            values.add(((Binary) value).toStringUsingUTF8());
                        }
                        break;
                    }
                    // Binary default as fall through to bytes
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                    for (int r = 0; r < reps; r++) {
                        Object value = group.getObject(fieldIndex, r);
                        assert value instanceof Binary : "Unexpected Parquet value type for binary, int96 and fixed-length byte array: " + value.getClass();
                        values.add(((Binary) value).getBytes());
                    }
                    break;
                default:
                    for (int r = 0; r < reps; r++) {
                        Object value = group.getObject(fieldIndex, r);
                        assert value.getClass().getPackage().getName().equals("java.lang") : "Unexpected Parquet primitive value type: " + value.getClass();
                        values.add(value);
                    }
                    break;
            }
        }
        return values;
    }

    /**
     * Convert a Parquet Group's Field that is a "singleton" -- a required or optional-present value.
     */
    private Object convertFieldSingleton(final Schema parentSchema, final SimpleGroup group, final Type field, final int fieldIndex) {
        Object value = group.getObject(fieldIndex, 0);
        assert value != null : "Required value should be nonnull";
        Object kafkaValue;
        if (field instanceof GroupType) {
            // When the singleton itself is a Group, recurs into {@link #convert(Schema, SimpleGroup) convert} method.
            assert value instanceof SimpleGroup : "Unsupported Parquet group value type: " + value.getClass();
            Schema kafkaFieldSchema = parentSchema.fields().get(fieldIndex).schema();
            kafkaValue = convert(kafkaFieldSchema, (SimpleGroup) value);
        } else {
            // Primitive
            assert field instanceof PrimitiveType : "Unexpected Parquet field type: " + field.getClass();
            switch (((PrimitiveType) field).getPrimitiveTypeName()) {
                case BINARY:
                    LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();
                    if (LogicalTypeAnnotation.stringType().equals(logicalType)) {
                        assert value instanceof Binary
                                : "Unexpected Parquet value type for UTF-8 String: " + value.getClass();
                        kafkaValue = ((Binary) value).toStringUsingUTF8();
                        break;
                    }
                    // Binary default as fall through to bytes
                case INT96:
                case FIXED_LEN_BYTE_ARRAY:
                    assert value instanceof Binary : "Unexpected Parquet value type for binary, int96 and fixed-length byte array: " + value.getClass();
                    kafkaValue = ((Binary) value).getBytes();
                    break;
                default:
                    assert value.getClass().getPackage().getName().equals("java.lang") : "Unexpected Parquet primitive value type: " + value.getClass();
                    kafkaValue = value;
                    break;
            }
        }
        assert kafkaValue != null;
        return kafkaValue;
    }
}
