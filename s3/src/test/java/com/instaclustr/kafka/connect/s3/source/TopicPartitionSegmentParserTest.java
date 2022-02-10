package com.instaclustr.kafka.connect.s3.source;

import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.instaclustr.kafka.connect.s3.source.EndOfSourceStreamException;
import com.instaclustr.kafka.connect.s3.sink.TopicPartitionBuffer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class TopicPartitionSegmentParserTest {
    public static final String LAST_READ_OFFSET = "lastReadOffset";
    String s3ObjectKey = "prefix/test/0/0000000000000000002-0000000000000000004";

    @Test
    public void givenWellFormulatedDataSingleRecord() throws Exception {
        TopicPartitionBuffer topicPartitionBuffer = new TopicPartitionBuffer("test", 0);
        byte[] key = "{\"x\":\"11\"}".getBytes();
        byte[] value = "{\"y\":\"22\"}".getBytes();

        SinkRecord record = new SinkRecord("test", 0, Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value, 2, 64L, TimestampType.NO_TIMESTAMP_TYPE);
        topicPartitionBuffer.putRecord(record);

        InputStream dataInputStream = topicPartitionBuffer.getInputStream();
        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(dataInputStream, s3ObjectKey, "");

        Assert.assertEquals(topicPartitionBuffer.topic, "test");
        Assert.assertEquals(topicPartitionBuffer.partition, 0);
        Assert.assertEquals(topicPartitionBuffer.getEndOffset(), 2L);
        Assert.assertEquals(topicPartitionBuffer.getStartOffset(), 2L);

        SourceRecord firstRecord = topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        Assert.assertEquals(firstRecord.sourceOffset().get(LAST_READ_OFFSET), 2L);
        Assert.assertEquals((byte[]) firstRecord.key(), key);
        Assert.assertEquals((byte[]) firstRecord.value(), value);
        Assert.assertEquals(firstRecord.timestamp(), record.timestamp());
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

        SourceRecord firstRecord = topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        Assert.assertEquals(firstRecord.sourceOffset().get(LAST_READ_OFFSET), 2L);
        Assert.assertNull(firstRecord.key());
        Assert.assertNull(firstRecord.value());
        Assert.assertEquals(firstRecord.timestamp(), record1.timestamp());


        SourceRecord secondRecord = topicPartitionSegmentParser.getNextRecord(5L, TimeUnit.SECONDS);
        Assert.assertEquals(secondRecord.sourceOffset().get(LAST_READ_OFFSET), 3L);
        Assert.assertNull(firstRecord.key());
        Assert.assertNull(firstRecord.value());
        Assert.assertEquals(secondRecord.timestamp(), record2.timestamp());


        SourceRecord thirdRecord = topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS);
        Assert.assertEquals(thirdRecord.sourceOffset().get(LAST_READ_OFFSET), 4L);
        Assert.assertEquals((byte[]) thirdRecord.key(), key);
        Assert.assertEquals((byte[]) thirdRecord.value(), value);
        Assert.assertEquals(thirdRecord.timestamp(), record3.timestamp());
    }

    @Test
    public void givenNonResponsiveStreamTriggerTimeoutOnDefinedTimePeriod() throws IOException {
        PipedOutputStream pipedOutputStream = null;
        try (PipedInputStream empty = new PipedInputStream(1)) {
            pipedOutputStream = new PipedOutputStream(empty); //making sure we just have an unresponsive stream and not throwing an ioexception
            TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(empty, s3ObjectKey, "");
            Assert.expectThrows(TimeoutException.class, () -> topicPartitionSegmentParser.getNextRecord(5L, TimeUnit.MILLISECONDS));
        } finally {
            if (pipedOutputStream != null) {
                pipedOutputStream.close();
            }
        }
    }

//    @Test
//    public void returnNullOnEofException() throws Exception {
//        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(InputStream.nullInputStream(), s3ObjectKey, "");
//        Assert.assertNull(topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS));
//        Assert.assertNull(topicPartitionSegmentParser.getNextRecord(1L, TimeUnit.SECONDS));
//        topicPartitionSegmentParser.closeResources();
//    }

//    @Test
//    public void multipleCreationOfParsersInALoop() throws Exception {
//        //test if there is a thread leak
//        for (int i = 0; i < 5000; i++) {
//            TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(InputStream.nullInputStream(), s3ObjectKey, "");
//            Assert.assertNull(topicPartitionSegmentParser.getNextRecord(100L, TimeUnit.MILLISECONDS));
//            topicPartitionSegmentParser.closeResources();
//        }
//    }
//
//    @Test
//    public void givenClosedStreamThrowIoException() throws IOException {
//        InputStream nullInputStream = InputStream.nullInputStream();
//        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(nullInputStream, s3ObjectKey, "");
//        nullInputStream.close();
//        try {
//            topicPartitionSegmentParser.getNextRecord(5L, TimeUnit.SECONDS);
//        } catch (Exception e) {
//            System.out.println("EXC:" + e);
//            Assert.assertTrue(e instanceof ExecutionException);
//            Assert.assertTrue(e.getCause() instanceof IOException);
//        }
//    }
//


}