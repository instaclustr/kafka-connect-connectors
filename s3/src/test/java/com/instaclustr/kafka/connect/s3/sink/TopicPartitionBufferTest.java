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

    @Test
    public void noSpaceForVersionInformation() throws IOException, RecordOutOfOrderException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 36);
        byte[] key = KEY_STRING.getBytes();
        byte[] value = VALUE_STRING.getBytes();
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);

        try {
            topicPartitionBuffer.putRecord(record);
            Assert.fail("Exception expected");
        }
        catch (MaxBufferSizeExceededException ex) {
            // Expected
        }
    }

    @Test
    public void putRecordOnce() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 40);
        byte[] key = KEY_STRING.getBytes();
        byte[] value = VALUE_STRING.getBytes();
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);

        byte[] expected = new byte[]{0, 0, 0, 0 // version
                , 0, 0, 0, 0, 0, 0, 0, 2 // offset
                , 0, 0, 0, 0, 0, 0, 0, 64 // timestamp
                , 0, 0, 0, 3 // key length
                , 0, 0, 0, 5 // value length
                , 0, 0, 0, 0 //header count
                , 'k', 'e', 'y'
                , 'v', 'a', 'l', 'u', 'e'
        };

        topicPartitionBuffer.putRecord(record);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);
        byte[] byteResult = topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength());
        Assert.assertEquals(byteResult, expected);
    }

    @Test
    public void putRecordTwice() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 76);
        byte[] key = KEY_STRING.getBytes();
        byte[] value = VALUE_STRING.getBytes();
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        SinkRecord record2 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 3, 68L, TimestampType.NO_TIMESTAMP_TYPE);

        byte[] expected = new byte[]{0, 0, 0, 0 // version
                , 0, 0, 0, 0, 0, 0, 0, 2 // offset
                , 0, 0, 0, 0, 0, 0, 0, 64 // timestamp
                , 0, 0, 0, 3 // key length
                , 0, 0, 0, 5 // value length
                , 0, 0, 0, 0 //header length
                , 'k', 'e', 'y'
                , 'v', 'a', 'l', 'u', 'e'
                , 0, 0, 0, 0, 0, 0, 0, 3 // offset
                , 0, 0, 0, 0, 0, 0, 0, 68 // timestamp
                , 0, 0, 0, 3 // key length
                , 0, 0, 0, 5 // value length
                , 0, 0, 0, 0 //header length
                , 'k', 'e', 'y'
                , 'v', 'a', 'l', 'u', 'e'
        };

        topicPartitionBuffer.putRecord(record);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), 40);
        topicPartitionBuffer.putRecord(record2);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);
        byte[] byteResult = topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength());
        Assert.assertEquals(byteResult, expected);

    }

    @Test
    public void putRecordThrice() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 76);
        byte[] key = KEY_STRING.getBytes();
        byte[] value = VALUE_STRING.getBytes();
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        SinkRecord record2 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 3, 68L, TimestampType.NO_TIMESTAMP_TYPE);
        SinkRecord record3 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 4, 72L, TimestampType.NO_TIMESTAMP_TYPE);

        byte[] expected = new byte[]{0, 0, 0, 0 // version
                , 0, 0, 0, 0, 0, 0, 0, 2 // offset
                , 0, 0, 0, 0, 0, 0, 0, 64 // timestamp
                , 0, 0, 0, 3 // key length
                , 0, 0, 0, 5 // value length
                , 0, 0, 0, 0 //header length
                , 'k', 'e', 'y'
                , 'v', 'a', 'l', 'u', 'e'
                , 0, 0, 0, 0, 0, 0, 0, 3 // offset
                , 0, 0, 0, 0, 0, 0, 0, 68 // timestamp
                , 0, 0, 0, 3 // key length
                , 0, 0, 0, 5 // value length
                , 0, 0, 0, 0 //header length
                , 'k', 'e', 'y'
                , 'v', 'a', 'l', 'u', 'e'
        };

        topicPartitionBuffer.putRecord(record);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), 40);

        topicPartitionBuffer.putRecord(record2);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);

        try {
            topicPartitionBuffer.putRecord(record3);
            Assert.fail("exception expected");
        } catch (MaxBufferSizeExceededException ex) {
            // Expected
        }
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);

        byte[] byteResult = topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength());
        Assert.assertEquals(byteResult, expected);

    }

    @Test
    public void putRecordWithHeaderData() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 80);
        Headers headers = new ConnectHeaders();
        headers.addBytes("header", new byte[]{1,1});
        headers.addBoolean("bool", true);
        SinkRecord record = new SinkRecord(
                TOPIC,
                0,
                Schema.OPTIONAL_BYTES_SCHEMA,
                new byte[]{1,2,3},
                Schema.OPTIONAL_BYTES_SCHEMA,
                new byte[]{0,0,0},
                2,
                64L,
                TimestampType.NO_TIMESTAMP_TYPE,
                headers);
        byte[] expected = new byte[]{
                0, 0, 0, 0, //version
                0, 0, 0, 0, 0, 0, 0, 2, //offset
                0, 0, 0, 0, 0, 0, 0, 64, //timestamp
                0, 0, 0, 3, //key length
                0, 0, 0, 3, //value length
                0, 0, 0, 2, //header length
                1, 2, 3, //key
                0, 0, 0, //value
                0, 0, 0, 6, 104, 101, 97, 100, 101, 114, //header1 key length and value
                0, 0, 0, 4, 65, 81, 69, 61, //header1 value length and data
                0, 0, 0, 4, 98, 111, 111, 108,
                0, 0, 0, 4, 116, 114, 117, 101
        };
        topicPartitionBuffer.putRecord(record);
        Assert.assertEquals(topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength()), expected);
    }

}
