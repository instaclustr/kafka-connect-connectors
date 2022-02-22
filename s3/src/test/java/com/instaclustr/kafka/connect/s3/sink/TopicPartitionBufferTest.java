package com.instaclustr.kafka.connect.s3.sink;


import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;

public class TopicPartitionBufferTest {

    public static final String TOPIC = "topic";
    public static final String KEY_JSON = "{\"x\":\"11\"}";
    public static final String VALUE_JSON = "{\"y\":\"22\"}";

    @Test
    public void noSpaceForVersionInformation() throws IOException, RecordOutOfOrderException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 14);
        byte[] key = KEY_JSON.getBytes();
        byte[] value = VALUE_JSON.getBytes();
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);

        try {
            topicPartitionBuffer.putRecord(record);
            Assert.fail("Exception expected");
        } catch (MaxBufferSizeExceededException ex) {
            // Expected
        }
    }

    @Test
    public void putRecordOnce() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 57);
        byte[] key = KEY_JSON.getBytes();
        byte[] value = VALUE_JSON.getBytes();
        SinkRecord record = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);

        byte[] expected = new byte[]{'{'
                , '"', 'k', '"', ':', '"', '{', '\\', '"', 'x', '\\', '"', ':', '\\', '"', '1', '1', '\\', '"', '}', '"', ','
                , '"', 'v', '"', ':', '"', '{', '\\', '"', 'y', '\\', '"', ':', '\\', '"', '2', '2', '\\', '"', '}', '"', ','
                , '"', 't', '"', ':', '6', '4', ','
                , '"', 'o', '"', ':', '2'
                , '}'
                , '\n'
        };

        topicPartitionBuffer.putRecord(record);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);
        byte[] byteResult = topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength());
        Assert.assertEquals(byteResult, expected);
    }

    @Test
    public void putRecordTwice() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 114);
        byte[] key = KEY_JSON.getBytes();
        byte[] value = VALUE_JSON.getBytes();
        SinkRecord record1 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        SinkRecord record2 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 3, 68L, TimestampType.NO_TIMESTAMP_TYPE);

        byte[] expected = new byte[]{'{'
                , '"', 'k', '"', ':', '"', '{', '\\', '"', 'x', '\\', '"', ':', '\\', '"', '1', '1', '\\', '"', '}', '"', ','
                , '"', 'v', '"', ':', '"', '{', '\\', '"', 'y', '\\', '"', ':', '\\', '"', '2', '2', '\\', '"', '}', '"', ','
                , '"', 't', '"', ':', '6', '4', ','
                , '"', 'o', '"', ':', '2'
                , '}'
                , '\n'
                , '{'
                , '"', 'k', '"', ':', '"', '{', '\\', '"', 'x', '\\', '"', ':', '\\', '"', '1', '1', '\\', '"', '}', '"', ','
                , '"', 'v', '"', ':', '"', '{', '\\', '"', 'y', '\\', '"', ':', '\\', '"', '2', '2', '\\', '"', '}', '"', ','
                , '"', 't', '"', ':', '6', '8', ','
                , '"', 'o', '"', ':', '3'
                , '}'
                , '\n'
        };

        topicPartitionBuffer.putRecord(record1);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), 57);
        topicPartitionBuffer.putRecord(record2);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), expected.length);
        byte[] byteResult = topicPartitionBuffer.getInputStream().readNBytes(topicPartitionBuffer.getInputStreamLength());
        Assert.assertEquals(byteResult, expected);
    }

    @Test
    public void putRecordThrice() throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer(TOPIC, 0, 114);
        byte[] key = KEY_JSON.getBytes();
        byte[] value = VALUE_JSON.getBytes();
        SinkRecord record1 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        SinkRecord record2 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 3, 68L, TimestampType.NO_TIMESTAMP_TYPE);
        SinkRecord record3 = new SinkRecord(TOPIC, 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 4, 72L, TimestampType.NO_TIMESTAMP_TYPE);

        byte[] expected = new byte[]{'{'
                , '"', 'k', '"', ':', '"', '{', '\\', '"', 'x', '\\', '"', ':', '\\', '"', '1', '1', '\\', '"', '}', '"', ','
                , '"', 'v', '"', ':', '"', '{', '\\', '"', 'y', '\\', '"', ':', '\\', '"', '2', '2', '\\', '"', '}', '"', ','
                , '"', 't', '"', ':', '6', '4', ','
                , '"', 'o', '"', ':', '2'
                , '}'
                , '\n'
                , '{'
                , '"', 'k', '"', ':', '"', '{', '\\', '"', 'x', '\\', '"', ':', '\\', '"', '1', '1', '\\', '"', '}', '"', ','
                , '"', 'v', '"', ':', '"', '{', '\\', '"', 'y', '\\', '"', ':', '\\', '"', '2', '2', '\\', '"', '}', '"', ','
                , '"', 't', '"', ':', '6', '8', ','
                , '"', 'o', '"', ':', '3'
                , '}'
                , '\n'
        };

        topicPartitionBuffer.putRecord(record1);
        Assert.assertEquals(topicPartitionBuffer.getInputStreamLength(), 57);

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
}
