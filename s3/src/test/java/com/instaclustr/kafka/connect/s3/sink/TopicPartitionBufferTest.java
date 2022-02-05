package com.instaclustr.kafka.connect.s3.sink;


import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class TopicPartitionBufferTest {

    public static final String TOPIC = "topic";
    public static final String KEY_STRING = "key";
    public static final String VALUE_STRING = "value";

//    @Test
//    public void noSpaceForVersionInformation() throws IOException, RecordOutOfOrderException {
//        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 14);
//        byte[] key = KEY_STRING.getBytes();
//        byte[] value = VALUE_STRING.getBytes();
//        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_STRING_SCHEMA, key, Schema.OPTIONAL_STRING_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
//
//        try {
//            topicPartitionBuffer.putRecord(record);
//            Assert.fail("Exception expected");
//        } catch (MaxBufferSizeExceededException ex) {
//            // Expected
//        }
//    }

    @Test
    public void putRecordOnce() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 18);
        byte[] key = KEY_STRING.getBytes();
        byte[] value = VALUE_STRING.getBytes();
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);

        byte[] expected = new byte[]{0,0,0,0 // version
                , '2' // offset
                , '6', '4' // timestamp
                , '3' // key length
                , '5' // value length
                , 'k', 'e', 'y'
                , 'v', 'a', 'l', 'u', 'e'
                , '\n'
        };

        topicPartitionBuffer.putRecord(record);
        System.out.println(">>>>>Expected: " + expected);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);
        byte[] byteResult = topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength());
        Assert.assertEquals(byteResult, expected);
    }

//    @Test
//    public void putRecordTwice() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
//        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 29);
//        byte[] key = KEY_STRING.getBytes();
//        byte[] value = VALUE_STRING.getBytes();
//        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
//        SinkRecord record2 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 3, 68L, TimestampType.NO_TIMESTAMP_TYPE);
//
//        byte[] expected = new byte[]{'0' // version
//                , '2' // offset
//                , '6', '4' // timestamp
//                , '3' // key length
//                , '5' // value length
//                , 'k', 'e', 'y'
//                , 'v', 'a', 'l', 'u', 'e'
//                , '\n'
//                , '3' // offset
//                , '6', '8' // timestamp
//                , '3' // key length
//                , '5' // value length
//                , 'k', 'e', 'y'
//                , 'v', 'a', 'l', 'u', 'e'
//                , '\n'
//        };
//
//        topicPartitionBuffer.putRecord(record);
//        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), 15);
//        topicPartitionBuffer.putRecord(record2);
//        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);
//        byte[] byteResult = topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength());
//        Assert.assertEquals(byteResult, expected);
//
//    }
//
//    @Test
//    public void putRecordThrice() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
//        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 29);
//        byte[] key = KEY_STRING.getBytes();
//        byte[] value = VALUE_STRING.getBytes();
//        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
//        SinkRecord record2 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 3, 68L, TimestampType.NO_TIMESTAMP_TYPE);
//        SinkRecord record3 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 4, 72L, TimestampType.NO_TIMESTAMP_TYPE);
//
//        byte[] expected = new byte[]{'0' // version
//                , '2' // offset
//                , '6','4' // timestamp
//                , '3' // key length
//                , '5' // value length
//                , 'k', 'e', 'y'
//                , 'v', 'a', 'l', 'u', 'e'
//                , '\n'
//                , '3' // offset
//                , '6','8' // timestamp
//                , '3' // key length
//                , '5' // value length
//                , 'k', 'e', 'y'
//                , 'v', 'a', 'l', 'u', 'e'
//                , '\n'
//        };
//
//        topicPartitionBuffer.putRecord(record);
//        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), 15);
//
//        topicPartitionBuffer.putRecord(record2);
//        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);
//
//        try {
//            topicPartitionBuffer.putRecord(record3);
//            Assert.fail("exception expected");
//        } catch (MaxBufferSizeExceededException ex) {
//            // Expected
//        }
//        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);
//
//        byte[] byteResult = topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength());
//        Assert.assertEquals(byteResult, expected);
//
//    }
}
