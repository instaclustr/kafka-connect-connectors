package com.instaclustr.kafka.connect.stream.types.parquet;

import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.example.Paper;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.*;

public class ParquetKafkaDataConverterTest {
    @Test
    public void schema() {
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();

        Struct result = converter.convert(Paper.r1);

        ParquetKafkaTypeConverter typeConverter = new ParquetKafkaTypeConverter();
        assertTrue(Paper.r1.getType() instanceof MessageType);
        assertEquals(result.schema(), ((MessageType) Paper.r1.getType()).convertWith(typeConverter));
    }

    @Test
    public void requiredPrimitive() {
        //// r1
        // DocId: 10 // tested
        // Name
        //  Language
        //    Country: 'us'
        //  Language
        // Name
        // Name
        //  Language
        //    Country: 'gb'
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
        Struct result = converter.convert(Paper.pr1);

        // Document.DocId
        assertEquals(result.get("DocId"), Paper.pr1.getObject("DocId", 0));
    }

    @Test
    public void nestedRepeatedGroups() {
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();

        // message Document {
        //     required int64 DocId;
        //     repeated group Name {  // Tested
        //         repeated group Language { // Tested
        //             optional binary Country;
        //         }
        //     }
        // }
        //// r1
        // DocId: 10
        // Name
        //  Language
        //    Country: 'us'
        //  Language
        // Name
        // Name
        //  Language
        //    Country: 'gb'
        Struct result = converter.convert(Paper.pr1);

        // Document.Name[]
        assertTrue(result.get("Name") instanceof List);
        List<Struct> names = result.getArray("Name");
        assertEquals(names.size(), 3);

        // Document.Name[0].Language[0]
        List<Struct> languages =  names.get(0).getArray("Language");
        assertEquals(languages.size(), 2);
        assertEquals(languages.get(0).getBytes("Country"),
                Paper.pr1.getGroup("Name", 0)
                        .getGroup("Language", 0)
                        .getBinary("Country", 0).getBytes());

        // Document.Name[0].Language[1]
        assertNull(languages.get(1).get("Country"));

        // Document.Name[1]
        assertTrue(names.get(1).get("Language") instanceof List);
        assertTrue(names.get(1).getArray("Language").isEmpty());

        // Document.Name[2].Language[0]
        languages = names.get(2).getArray("Language");
        assertEquals(languages.size(), 1);
        assertEquals(languages.get(0).getBytes("Country"),
                Paper.pr1.getGroup("Name", 2)
                        .getGroup("Language", 0)
                        .getBinary("Country", 0).getBytes());
    }

    @Test
    public void emptyGroup() {
        //// r2
        // DocId: 20
        // Name // Tested
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
        Struct result = converter.convert(Paper.pr2);

        // Document.Name
        Struct name = result.<Struct>getArray("Name").get(0);
        List<Struct> kafkaNameLanguage = (List<Struct>) name.get(name.schema().fields().get(0));
        var parquetNameLanguage = (SimpleGroup) Paper.pr2.getObject("Name", 0);
        assertEquals(kafkaNameLanguage.size(), parquetNameLanguage.getFieldRepetitionCount("Language"));
    }

    @Test
    public void repeatedPrimitive() {
        //// r2
        // DocId: 20
        // Links
        // Backward: 10 // tested
        // Backward: 30 // tested
        // Forward:  80 // tested
        // Name
        // Url: 'http://C'
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
        Struct result = converter.convert(Paper.r2);

        // Document.Links.Backward
        List<Struct> backward = result.getStruct("Links").getArray("Backward");
        assertEquals(backward.size(), 2);
        assertEquals(backward.get(0), Paper.r2.getGroup("Links", 0).getLong("Backward", 0));
        assertEquals(backward.get(1), Paper.r2.getGroup("Links", 0).getLong("Backward", 1));

        // Document.Links.Forward
        List<Struct> forward = result.getStruct("Links").getArray("Forward");
        assertEquals(forward.size(), 1);
        assertEquals(forward.get(0), Paper.r2.getGroup("Links", 0).getLong("Forward", 0));
    }

    @Test
    public void optionalPrimitive() {
        //// r1
        // DocId: 10
        // Name
        //  Language
        //    Country: 'us' // Tested
        //  Language
        // Name
        // Name
        //  Language
        //    Country: 'gb' // Tested
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
        Struct result = converter.convert(Paper.pr1);

        // Document.Name[0].Language[0].Country
        List<Struct> name = result.getArray("Name");
        List<Struct> language = name.get(0).getArray("Language");
        assertEquals(language.get(0).getBytes("Country"),
                Paper.pr1.getGroup("Name", 0)
                        .getGroup("Language", 0)
                        .getBinary("Country", 0).getBytes());

        // Document.Name[0].Language[1].Country
        assertNull(language.get(1).getBytes("Country"));
    }

    @Test
    public void utf8String() {
        MessageType schema = new MessageType(
                "Document",
                new PrimitiveType(REQUIRED, BINARY, "DocName", OriginalType.UTF8));
        SimpleGroup group = new SimpleGroup(schema);
        group.add("DocName", "myString");
        
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
        Struct result = converter.convert(group);

        // Document.DocName
        Object actual = result.get("DocName");
        assertTrue(actual instanceof String);
        assertEquals(actual, group.getString("DocName", 0));
    }
}
