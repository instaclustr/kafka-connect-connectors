package com.instaclustr.kafka.connect.stream.types.parquet;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.parquet.example.Paper;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class ParquetKafkaTypeConverterTest {
    
    @Test
    public void nestedType() {
        final ParquetKafkaTypeConverter converter = new ParquetKafkaTypeConverter();

        final Schema kafkaType = Paper.schema.convertWith(converter);

        assertEquals(kafkaType.name(), "Document");
        assertEquals(kafkaType.fields().size(), 3);
        assertEquals(kafkaType.type(), Schema.Type.STRUCT);
        assertFalse(kafkaType.isOptional());

        // DocId
        final Field docId = kafkaType.fields().get(0);
        checkTypeName(docId, "DocId");
        assertFalse(docId.schema().isOptional());
        assertEquals(docId.schema().type(), Schema.Type.INT64);

        // Links
        final Field links = kafkaType.fields().get(1);
        checkTypeName(links, "Links");
        assertTrue(links.schema().isOptional());
        assertEquals(links.schema().type(), Schema.Type.STRUCT);
        assertEquals(links.schema().fields().size(), 2);

        final Field backward  = links.schema().fields().get(0);
        checkTypeName(backward, "Backward");
        assertTrue(backward.schema().isOptional());
        assertEquals(backward.schema().type(), Schema.Type.ARRAY);
        assertEquals(backward.schema().valueSchema().type(), Schema.Type.INT64);

        final Field forward = links.schema().fields().get(1);
        checkTypeName(forward, "Forward");
        assertTrue(forward.schema().isOptional());
        assertEquals(forward.schema().type(), Schema.Type.ARRAY);
        assertEquals(forward.schema().valueSchema().type(), Schema.Type.INT64);

        // Name
        final Field name = kafkaType.fields().get(2);
        checkTypeName(name, "Name");
        assertEquals(name.schema().type(), Schema.Type.ARRAY);
        assertEquals(name.schema().valueSchema().name(), "Name");
        assertEquals(name.schema().valueSchema().type(), Schema.Type.STRUCT);
        assertEquals(name.schema().valueSchema().fields().size(), 2);
        assertTrue(name.schema().isOptional());

        final Field language = name.schema().valueSchema().fields().get(0);
        checkTypeName(language, "Language");
        assertEquals(language.schema().type(), Schema.Type.ARRAY);
        assertEquals(language.schema().valueSchema().name(), "Language");
        assertEquals(language.schema().valueSchema().type(), Schema.Type.STRUCT);
        assertEquals(language.schema().valueSchema().fields().size(), 2);
        assertTrue(language.schema().isOptional());

        final Field url = name.schema().valueSchema().fields().get(1);
        checkTypeName(url, "Url");
        assertEquals(url.schema().type(), Schema.Type.BYTES);
        assertTrue(url.schema().isOptional());

        final Field code = language.schema().valueSchema().fields().get(0);
        checkTypeName(code, "Code");
        assertEquals(code.schema().type(), Schema.Type.BYTES);
        assertFalse(code.schema().isOptional());

        final Field country = language.schema().valueSchema().fields().get(1);
        checkTypeName(country, "Country");
        assertEquals(country.schema().type(), Schema.Type.BYTES);
        assertTrue(country.schema().isOptional());
    }

    @Test
    public void primitiveType() {
        checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName.INT64, Schema.Type.INT64);
        checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName.INT32, Schema.Type.INT32);
        checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName.BOOLEAN, Schema.Type.BOOLEAN);
        checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName.BINARY, Schema.Type.BYTES);
        checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName.FLOAT, Schema.Type.FLOAT32);
        checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName.DOUBLE, Schema.Type.FLOAT64);
        checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName.INT96, Schema.Type.BYTES);
        checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Schema.Type.BYTES);
    }

    @Test
    public void repetitionLevel() {
        checkOptional(Type.Repetition.REQUIRED, false);
        checkOptional(Type.Repetition.REPEATED, true);
        checkOptional(Type.Repetition.OPTIONAL, true);
    }

    private static void checkOptional(Type.Repetition rep, boolean expectedOptional) {
        Schema kafkaSchema = kafkaSchemaFrom(PrimitiveType.PrimitiveTypeName.INT64, rep);
        assertEquals(kafkaSchema.isOptional(), expectedOptional);
    }

    private static void checkPrimitiveTypes(PrimitiveType.PrimitiveTypeName parquetTypeName, Schema.Type kafkaType) {
        Schema kafkaSchema = kafkaSchemaFrom(parquetTypeName, Type.Repetition.REQUIRED);
        assertEquals(kafkaSchema, SchemaBuilder.type(kafkaType).name("myType").build());
    }

    private static void checkTypeName(Field field, String name) {
        assertEquals(field.name(), name);
        assertEquals(field.schema().name(), name);
    }

    private static Schema kafkaSchemaFrom(PrimitiveType.PrimitiveTypeName parquetTypeName, Type.Repetition rep) {
        PrimitiveType parquetType = new PrimitiveType(rep, parquetTypeName, "myType");
        final ParquetKafkaTypeConverter converter = new ParquetKafkaTypeConverter();
        return parquetType.convert(null, converter);
    }
}
