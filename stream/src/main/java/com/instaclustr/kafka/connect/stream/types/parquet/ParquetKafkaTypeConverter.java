package com.instaclustr.kafka.connect.stream.types.parquet;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.TypeConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;

public class ParquetKafkaTypeConverter implements TypeConverter<Schema> {

    private static final Map<PrimitiveTypeName, Schema.Type> P2K_PRIMITIVE_TYPE = Map.of(
        PrimitiveTypeName.INT64,                Schema.Type.INT64,
        PrimitiveTypeName.INT32,                Schema.Type.INT32,
        PrimitiveTypeName.BOOLEAN,              Schema.Type.BOOLEAN,
        PrimitiveTypeName.BINARY,               Schema.Type.BYTES,
        PrimitiveTypeName.FLOAT,                Schema.Type.FLOAT32,
        PrimitiveTypeName.DOUBLE,               Schema.Type.FLOAT64,
        PrimitiveTypeName.INT96,                Schema.Type.BYTES,
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Schema.Type.BYTES
    );

    private static final Map<LogicalTypeAnnotation, Schema.Type> LOGICAL_TYPE = Map.of(
        LogicalTypeAnnotation.stringType(), Schema.Type.STRING
    );

    @Override
    public Schema convertPrimitiveType(List<GroupType> path, PrimitiveType primitiveType) {
        switch (primitiveType.getRepetition()) {
            case REQUIRED:
                return schemaBuilderOf(primitiveType).build();

            case REPEATED:
                return SchemaBuilder.array(schemaBuilderOf(primitiveType).build())
                        .name(primitiveType.getName())
                        .optional()
                        .build();

            case OPTIONAL:
                return schemaBuilderOf(primitiveType).optional().build();

            default:
                throw new ConnectException("Unexpected type repetition");
        }
    }

    private static SchemaBuilder schemaBuilderOf(PrimitiveType primitiveType) {
        PrimitiveTypeName parquetType = primitiveType.getPrimitiveTypeName();
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        Schema.Type kafkaType;
        if (logicalType != null && LOGICAL_TYPE.containsKey(logicalType)) {
            kafkaType = Objects.requireNonNull(LOGICAL_TYPE.get(logicalType),
                    "Logical type mapping is undefined for Parquet: " + logicalType);
        } else {
            kafkaType = Objects.requireNonNull(P2K_PRIMITIVE_TYPE.get(parquetType),
                    "Type mapping is undefined for Parquet: " + parquetType);
        }
        return SchemaBuilder.type(kafkaType).name(primitiveType.getName());
    }

    @Override
    public Schema convertGroupType(List<GroupType> path, GroupType groupType, List<Schema> children) {
        switch (groupType.getRepetition()) {
            case REQUIRED:
                return structBuilderOf(groupType, children).build();
            case REPEATED:
                return SchemaBuilder.array(structBuilderOf(groupType, children).build())
                        .name(groupType.getName())
                        .optional()
                        .build();
            case OPTIONAL:
                return structBuilderOf(groupType, children).optional().build();
            default:
                throw new ConverterError("Unexected type repetition");

        }
    }

    @Override
    public Schema convertMessageType(MessageType messageType, List<Schema> children) {
        return structBuilderOf(messageType, children).build();
    }

    private static SchemaBuilder structBuilderOf(Type type, List<Schema> children) {
        SchemaBuilder struct = SchemaBuilder.struct().name(type.getName());
        for (Schema c : children) {
            struct.field(c.name(), c);
        }
        return struct;
    }
}
