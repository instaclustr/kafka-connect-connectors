package com.instaclustr.kafka.connect.s3.source;

import com.instaclustr.kafka.connect.s3.sink.TopicPartitionBuffer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TopicPartitionSegmentParserTest {
    public static final String LAST_READ_OFFSET = "lastReadOffset";
    String s3ObjectKey = "prefix/test/0/0000000000000000002-0000000000000000004.txt";

    @Test
    public void serDesIsWorkingForWeirdCharacters() throws Exception{
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer("test", 0);
        byte[] key = "\"££:::::£`£\"".getBytes();
        byte[] value = "\"\n&\t&&&$مُنَاقَشَةُ سُبُلِ اِسَّْطْبِيقَاتُ الْحاسُوبِيَّة\"".getBytes();

        SinkRecord record = new SinkRecord("test", 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        topicPartitionBuffer.putRecord(record);

        InputStream dataInputStream = topicPartitionBuffer.getInputStream();
        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(dataInputStream, s3ObjectKey, "");

        Assert.assertEquals(topicPartitionBuffer.topic, "test");
        Assert.assertEquals(topicPartitionBuffer.partition, 0);
        Assert.assertEquals(topicPartitionBuffer.getEndOffset(), 2L);
        Assert.assertEquals(topicPartitionBuffer.getStartOffset(), 2L);

        SourceRecord sourceRecord = topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        Assert.assertEquals(sourceRecord.sourceOffset().get(LAST_READ_OFFSET), 2L);
        Assert.assertEquals((byte[]) sourceRecord.key(), key);
        Assert.assertEquals((byte[]) sourceRecord.value(), value);
        Assert.assertEquals(sourceRecord.timestamp(), record.timestamp());
    }

    @Test
    public void givenKeyAsJSValue() throws Exception {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer("test", 0);
        byte[] key = "\"hFCBaHIxQh\"".getBytes();
        byte[] value = "{\"y\":\"22\"}".getBytes();

        SinkRecord record = new SinkRecord("test", 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        topicPartitionBuffer.putRecord(record);

        InputStream dataInputStream = topicPartitionBuffer.getInputStream();
        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(dataInputStream, s3ObjectKey, "");

        Assert.assertEquals(topicPartitionBuffer.topic, "test");
        Assert.assertEquals(topicPartitionBuffer.partition, 0);
        Assert.assertEquals(topicPartitionBuffer.getEndOffset(), 2L);
        Assert.assertEquals(topicPartitionBuffer.getStartOffset(), 2L);

        SourceRecord sourceRecord = topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        Assert.assertEquals(sourceRecord.sourceOffset().get(LAST_READ_OFFSET), 2L);
        Assert.assertEquals((byte[]) sourceRecord.key(), key);
        Assert.assertEquals((byte[]) sourceRecord.value(), value);
        Assert.assertEquals(sourceRecord.timestamp(), record.timestamp());
    }

    @Test
    public void givenWellFormulatedLargeDataSingleRecord() throws Exception {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer("test", 0);
        byte[] key = "{\"x\":\"epuWQMjIgP\"}".getBytes();
        byte[] value = "{\"y\":\"22\"}".getBytes();

        SinkRecord record = new SinkRecord("test", 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        topicPartitionBuffer.putRecord(record);

        InputStream dataInputStream = topicPartitionBuffer.getInputStream();
        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(dataInputStream, s3ObjectKey, "");

        Assert.assertEquals(topicPartitionBuffer.topic, "test");
        Assert.assertEquals(topicPartitionBuffer.partition, 0);
        Assert.assertEquals(topicPartitionBuffer.getEndOffset(), 2L);
        Assert.assertEquals(topicPartitionBuffer.getStartOffset(), 2L);

        SourceRecord sourceRecord = topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        Assert.assertEquals(sourceRecord.sourceOffset().get(LAST_READ_OFFSET), 2L);
        Assert.assertEquals((byte[]) sourceRecord.key(), key);
        Assert.assertEquals((byte[]) sourceRecord.value(), value);
        Assert.assertEquals(sourceRecord.timestamp(), record.timestamp());
    }

    @Test
    public void givenWellFormulatedDataMultipleRecords() throws Exception {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer("test", 0);
        byte[] key = "{\"x\":\"11\"}".getBytes();
        byte[] value = "{\"y\":\"22\"}".getBytes();

        SinkRecord record1 = new SinkRecord("test", 0, Schema.OPTIONAL_BYTES_SCHEMA, null, Schema.OPTIONAL_BYTES_SCHEMA, null, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        SinkRecord record2 = new SinkRecord("test", 0, Schema.OPTIONAL_BYTES_SCHEMA, "".getBytes(), Schema.OPTIONAL_BYTES_SCHEMA, "".getBytes(), 3, 68L, TimestampType.NO_TIMESTAMP_TYPE, null);
        SinkRecord record3 = new SinkRecord("test", 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 4, 72L, TimestampType.NO_TIMESTAMP_TYPE);
        topicPartitionBuffer.putRecord(record1);
        topicPartitionBuffer.putRecord(record2);
        topicPartitionBuffer.putRecord(record3);
        InputStream dataInputStream = topicPartitionBuffer.getInputStream();
        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(dataInputStream, s3ObjectKey, "");

        Assert.assertEquals(topicPartitionBuffer.topic, "test");
        Assert.assertEquals(topicPartitionBuffer.partition, 0);
        Assert.assertEquals(topicPartitionBuffer.getEndOffset(), 4L);
        Assert.assertEquals(topicPartitionBuffer.getStartOffset(), 2L);

        SourceRecord firstSourceRecord = topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        Assert.assertEquals(firstSourceRecord.sourceOffset().get(LAST_READ_OFFSET), 2L);
        Assert.assertNull(firstSourceRecord.key());
        Assert.assertNull(firstSourceRecord.value());
        Assert.assertEquals(firstSourceRecord.timestamp(), record1.timestamp());


        SourceRecord secondSourceRecord = topicPartitionSegmentParser.getNextRecord(5L, TimeUnit.SECONDS);
        Assert.assertEquals(secondSourceRecord.sourceOffset().get(LAST_READ_OFFSET), 3L);
        Assert.assertNull(firstSourceRecord.key());
        Assert.assertNull(firstSourceRecord.value());
        Assert.assertEquals(secondSourceRecord.timestamp(), record2.timestamp());


        SourceRecord thirdSourceRecord = topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        Assert.assertEquals(thirdSourceRecord.sourceOffset().get(LAST_READ_OFFSET), 4L);
        Assert.assertEquals((byte[]) thirdSourceRecord.key(), key);
        Assert.assertEquals((byte[]) thirdSourceRecord.value(), value);
        Assert.assertEquals(thirdSourceRecord.timestamp(), record3.timestamp());
    }

    @Test
    public void givenNonResponsiveStreamRaiseIoException() throws Exception {
        PipedOutputStream pipedOutputStream = null;
        try (PipedInputStream empty = new PipedInputStream(1)) {
            pipedOutputStream = new PipedOutputStream(empty); //making sure we just have an unresponsive stream and not throwing an ioexception
            TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(empty, s3ObjectKey, "");
            topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ExecutionException);
            Assert.assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void givenClosedStreamThrowIoException() throws IOException {
        InputStream nullInputStream = InputStream.nullInputStream();
        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(nullInputStream, s3ObjectKey, "");
        nullInputStream.close();
        try {
            topicPartitionSegmentParser.getNextRecord(5L, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ExecutionException);
            Assert.assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void givenBadFormatS3ObjectKeyThrowException() {
        Assert.expectThrows(IllegalArgumentException.class, () -> new TopicPartitionSegmentParser(new DataInputStream(System.in), "bla", ""));
    }

}