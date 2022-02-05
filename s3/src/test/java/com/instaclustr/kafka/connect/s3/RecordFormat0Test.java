//package com.instaclustr.kafka.connect.s3;
//
//import com.instaclustr.kafka.connect.s3.sink.MaxBufferSizeExceededException;
//import org.apache.kafka.common.record.TimestampType;
//import org.apache.kafka.connect.data.Schema;
//import org.apache.kafka.connect.header.ConnectHeaders;
//import org.apache.kafka.connect.header.Header;
//import org.apache.kafka.connect.header.Headers;
//import org.apache.kafka.connect.sink.SinkRecord;
//import org.apache.kafka.connect.source.SourceRecord;
//import org.testng.Assert;
//import org.testng.annotations.BeforeMethod;
//import org.testng.annotations.Test;
//
//import java.io.*;
//
//public class RecordFormat0Test {
//
//    RecordFormat0 recordFormat;
//    ByteArrayOutputStream bos;
//    DataOutputStream dataOutputStream;
//
//    @BeforeMethod
//    public void setup() {
//        recordFormat = new RecordFormat0();
//        bos = new ByteArrayOutputStream();
//        dataOutputStream = new DataOutputStream(bos);
//    }
//
//    @Test
//    public void writeRecordTest() throws IOException, MaxBufferSizeExceededException {
//        byte[] key = new byte[]{1, 2, 3};
//        byte[] value = new byte[]{4, 5, 6, 7};
//
//        SinkRecord toWrite = new SinkRecord("test_topic", 0, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, value, 5, 17L, TimestampType.NO_TIMESTAMP_TYPE);
//
//        int written = recordFormat.writeRecord(dataOutputStream, toWrite, 13);
//        byte[] output = bos.toByteArray();
//
//        Assert.assertEquals(output.length, written);
//        Assert.assertEquals(output, new byte[]{
//                '5' // offset
//                , '1', '7' // timestamp
//                , '3' // key length
//                , '4' // value length
//                , 1, 2, 3 // key
//                , 4, 5, 6, 7 // value
//                , '\n'
//        });
//    }
//
//    @Test
//    public void tooLargeTest() throws IOException {
//        byte[] key = new byte[]{1, 2, 3};
//        byte[] value = new byte[]{4, 5, 6, 7};
//        SinkRecord toWrite = new SinkRecord("test_topic", 0, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, value, 5, 17L, TimestampType.NO_TIMESTAMP_TYPE);
//
//        try {
//            recordFormat.writeRecord(dataOutputStream, toWrite, 11);
//            Assert.fail("expected exception");
//        } catch (MaxBufferSizeExceededException ex) {
//            //expected exception
//        }
//    }
//
//    @Test
//    public void writeEmptiesTest() throws IOException, MaxBufferSizeExceededException {
//        byte[] key = new byte[0];
//        byte[] value = new byte[0];
//        SinkRecord toWrite = new SinkRecord("test_topic", 0, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, value, 5, 17L, TimestampType.NO_TIMESTAMP_TYPE);
//
//        int written = recordFormat.writeRecord(dataOutputStream, toWrite, 6);
//
//        byte[] output = bos.toByteArray();
//
//        Assert.assertEquals(output.length, written);
//        Assert.assertEquals(output, new byte[]{
//                '5' // offset
//                , '1','7' // timestamp
//                , '0' // key length
//                , '0' // value length
//                , '\n'
//        });
//    }
//
//    @Test
//    public void writeNullsTest() throws IOException, MaxBufferSizeExceededException {
//        SinkRecord toWrite = new SinkRecord("test_topic", 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, null, 4, 16L, TimestampType.NO_TIMESTAMP_TYPE);
//
//        int written = recordFormat.writeRecord(dataOutputStream, toWrite, 6);
//
//        byte[] output = bos.toByteArray();
//
//        Assert.assertEquals(output.length, written);
//        Assert.assertEquals(output, new byte[]{
//                '4' // offset
//                , '1', '6' // timestamp
//                , '0' // key length
//                , '0' // value length
//                , '\n'
//        });
//    }
//}
