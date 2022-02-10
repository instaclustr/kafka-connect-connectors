package com.instaclustr.kafka.connect.s3.source;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.instaclustr.kafka.connect.s3.TransferManagerProvider;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class AwsStorageSourceTaskTest {

    @Test
    public void givenNoDataNotHangOnPollCalls() {
        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader, 20, 10);
        for (int i = 0; i < 5; i++) {
            awsStorageSourceTask.poll();
        }
    }

    @Test
    public void testPausedPositionScanCallTriggerOnExpectedIntervals() {
        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader, 100, 50);
        for (int i = 0; i < 5; i++) {
            awsStorageSourceTask.poll();
        }
        verify(mockAwsSourceReader, times(2)).refreshPausedAwsReadPositions();
    }

    @Test
    public void givenNonResponsiveObjectStreamResetReadPosition() throws Exception {
        //this takes 10 seconds
        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
        TopicPartitionSegmentParser mockTopicPartitionSegmentParser = mock(TopicPartitionSegmentParser.class);

        doReturn(mockTopicPartitionSegmentParser).when(mockAwsSourceReader).getNextTopicPartitionSegmentParser();
        doReturn("test").when(mockTopicPartitionSegmentParser).getTopic();
        doReturn(0).when(mockTopicPartitionSegmentParser).getPartition();

        doThrow(new TimeoutException()).when(mockTopicPartitionSegmentParser).getNextRecord(any(), any());

        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader);
        awsStorageSourceTask.poll();

        verify(mockAwsSourceReader, times(1)).revertAwsReadPositionMarker("test/0");
    }

    @Test
    public void givenObjectStreamThatThrowsIOExceptionResetReadPosition() throws Exception {
        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
        TopicPartitionSegmentParser mockTopicPartitionSegmentParser = mock(TopicPartitionSegmentParser.class);

        doReturn(mockTopicPartitionSegmentParser).when(mockAwsSourceReader).getNextTopicPartitionSegmentParser();
        doReturn("test").when(mockTopicPartitionSegmentParser).getTopic();
        doReturn(0).when(mockTopicPartitionSegmentParser).getPartition();
        doThrow(new ExecutionException(new IOException())).when(mockTopicPartitionSegmentParser).getNextRecord(any(), any());

        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader);
        awsStorageSourceTask.poll();

        verify(mockAwsSourceReader, times(1)).revertAwsReadPositionMarker("test/0");
    }

    @Test
    public void givenObjectStreamThatEndsBeforeExpectedOffsetThrowException() throws Exception {
        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
        TopicPartitionSegmentParser mockTopicPartitionSegmentParser = mock(TopicPartitionSegmentParser.class);

        doReturn(mockTopicPartitionSegmentParser).when(mockAwsSourceReader).getNextTopicPartitionSegmentParser();
        doReturn("test").when(mockTopicPartitionSegmentParser).getTopic();
        doReturn(0).when(mockTopicPartitionSegmentParser).getPartition();
        doReturn(null).when(mockTopicPartitionSegmentParser).getNextRecord(any(), any());

        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader);
        Assert.expectThrows(MissingRecordsException.class, awsStorageSourceTask::poll);
        verify(mockAwsSourceReader, times(0)).revertAwsReadPositionMarker(any());
    }

    @Test
    public void givenObjectStreamThatGivesBadOffsetRecordThrowException() throws Exception {
        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
        TopicPartitionSegmentParser mockTopicPartitionSegmentParser = mock(TopicPartitionSegmentParser.class);

        doReturn(mockTopicPartitionSegmentParser).when(mockAwsSourceReader).getNextTopicPartitionSegmentParser();
        doReturn("test").when(mockTopicPartitionSegmentParser).getTopic();
        doReturn(0).when(mockTopicPartitionSegmentParser).getPartition();
        doReturn(1L).when(mockAwsSourceReader).getLastReadOffset(any());
        HashMap<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put("lastReadOffset", 5L);
        sourceOffset.put("s3ObjectKey", "test-key");

        doReturn(new SourceRecord(Collections.emptyMap(), sourceOffset, "test", 0, Schema.BYTES_SCHEMA, new byte[0])).when(mockTopicPartitionSegmentParser).getNextRecord(any(), any());

        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader);
        Assert.expectThrows(MissingRecordsException.class, awsStorageSourceTask::poll);
    }

//    @Test
//    public void givenAwsSdkThrowsServiceTypeAwsServerExceptionResetReadPosition() throws IOException {
//        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
//        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
//        S3ObjectInputStream s3ObjectInputStream = mock(S3ObjectInputStream.class);
//        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(s3ObjectInputStream,
//                "prefix/test/0/0000000000000000002-0000000000000000004", "");
//
//        doReturn(topicPartitionSegmentParser).when(mockAwsSourceReader).getNextTopicPartitionSegmentParser();
//        AmazonServiceException amazonServiceException = new AmazonServiceException("hello");
//        amazonServiceException.setErrorType(AmazonServiceException.ErrorType.Service);
//        doThrow(amazonServiceException).when(s3ObjectInputStream).read();
//
//        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader);
//        awsStorageSourceTask.poll();
//        verify(mockAwsSourceReader, times(1)).revertAwsReadPositionMarker(any());
//    }

//    @Test
//    public void givenAwsSdkThrowsRetryableAwsClientExceptionResetReadPosition() throws IOException {
//        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
//        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
//        S3ObjectInputStream s3ObjectInputStream = mock(S3ObjectInputStream.class);
//        TopicPartitionSegmentParser topicPartitionSegmentParser = new TopicPartitionSegmentParser(s3ObjectInputStream,
//                "prefix/test/0/0000000000000000002-0000000000000000004", "");
//
//        doReturn(topicPartitionSegmentParser).when(mockAwsSourceReader).getNextTopicPartitionSegmentParser();
//        AmazonClientException amazonClientException = new AmazonClientException("hello");
//        doThrow(amazonClientException).when(s3ObjectInputStream).read();
//
//        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader);
//        awsStorageSourceTask.poll();
//        verify(mockAwsSourceReader, times(1)).revertAwsReadPositionMarker(any());
//    }

    @Test
    public void recordProducerRateLimitTest() throws Exception {
        TransferManagerProvider mockTransferManagerProvider = mock(TransferManagerProvider.class);
        AwsSourceReader mockAwsSourceReader = mock(AwsSourceReader.class);
        TopicPartitionSegmentParser topicPartitionSegmentParser = mock(TopicPartitionSegmentParser.class);
        AwsStorageSourceTask awsStorageSourceTask = new AwsStorageSourceTask(mockTransferManagerProvider, mockAwsSourceReader);
        doReturn(topicPartitionSegmentParser).when(mockAwsSourceReader).getNextTopicPartitionSegmentParser();
        doReturn(-1L).when(mockAwsSourceReader).getLastReadOffset(any());
        doReturn(0L).when(topicPartitionSegmentParser).getEndOffset();
        HashMap<String, Long> lastOffsetInfoMap = new HashMap<>();
        lastOffsetInfoMap.put("lastReadOffset", 0L);
        doReturn(new SourceRecord(new HashMap<>(), lastOffsetInfoMap, "test", 0, Schema.BYTES_SCHEMA,
                new byte[0])).when(topicPartitionSegmentParser).getNextRecord(any(), any());
        ZonedDateTime startTime = ZonedDateTime.now();
        for (int i = 0; i < 1250; i++) {
            awsStorageSourceTask.poll();
        }
        long elapsedTimeSeconds = startTime.until(ZonedDateTime.now(), ChronoUnit.SECONDS);
        Assert.assertTrue(elapsedTimeSeconds >= 2);
    }
}