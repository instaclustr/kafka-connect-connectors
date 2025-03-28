package com.instaclustr.kafka.connect.s3.source;

import com.amazonaws.AmazonClientException;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.instaclustr.kafka.connect.s3.AwsConnectorStringFormats;
import com.instaclustr.kafka.connect.s3.AwsStorageConnectorCommonConfig;
import com.instaclustr.kafka.connect.s3.TransferManagerProvider;
import com.instaclustr.kafka.connect.s3.VersionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class AwsStorageSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(AwsStorageSourceTask.class);
    private long pausedQueueScanIntervalMs = 60000 * 5L;
    private long pollSleepIntervalMs = 1000;
    private TransferManagerProvider transferManagerProvider;
    private Map<String, String> configMap;
    private AwsSourceReader awsSourceReader;
    private long lastPausedQueueScanTimeStamp;
    private RateLimiter pollRecordRateLimiter;
    private LinkedList<SourceRecord> recordsToBeDelivered;

    public AwsStorageSourceTask() { //do not remove, kafka connect usage
        this.recordsToBeDelivered = new LinkedList<>();
    }

    public AwsStorageSourceTask(TransferManagerProvider transferManagerProvider, AwsSourceReader sourceReader) {
        this();
        this.transferManagerProvider = transferManagerProvider;
        this.awsSourceReader = sourceReader;
        this.lastPausedQueueScanTimeStamp = System.currentTimeMillis();
        this.configMap = Collections.emptyMap();
        this.pollRecordRateLimiter = RateLimiter.create(500);
    }

    public AwsStorageSourceTask(TransferManagerProvider transferManagerProvider, AwsSourceReader sourceReader, long pausedQueueScanIntervalMs, long pollSleepIntervalMs) {
        this(transferManagerProvider, sourceReader);
        this.pollSleepIntervalMs = pollSleepIntervalMs;
        this.pausedQueueScanIntervalMs = pausedQueueScanIntervalMs;
    }

    @Override
    public void start(Map<String, String> map) {
        this.configMap = map;
        this.transferManagerProvider = new TransferManagerProvider(this.configMap);
        String tasksString = map.getOrDefault(AwsStorageSourceConnector.WORKER_TASK_PARTITIONS_ENTRY, "").trim();
        List<String> topicPartitionList = StringUtils.isBlank(tasksString) ? Collections.emptyList() : Arrays.asList(tasksString.split(","));
        this.awsSourceReader = new AwsSourceReader(
                this.transferManagerProvider.get().getAmazonS3Client(),
                this.configMap.get(AwsStorageConnectorCommonConfig.BUCKET),
                AwsConnectorStringFormats.parseS3Prefix(this.configMap.getOrDefault(AwsStorageConnectorCommonConfig.S3_KEY_PREFIX, "")),
                this.configMap.getOrDefault(AwsStorageSourceConnector.SINK_TOPIC_PREFIX, ""),
                this.loadSourceConnectorTopicPartitionOffsets(topicPartitionList));
        if (topicPartitionList.isEmpty()) {
            log.info("No topic partitions assigned");
        } else {
            log.info("Assigned topics and partitions : {}", tasksString);
        }
        this.pollRecordRateLimiter = RateLimiter.create(Integer.parseInt(this.configMap.getOrDefault(AwsStorageSourceConnector.MAX_RECORDS_PER_SECOND, AwsStorageSourceConnector.MAX_RECORDS_PER_SECOND_DEFAULT)));
        lastPausedQueueScanTimeStamp = System.currentTimeMillis();
    }

    public Map<String, Map<String, Object>> loadSourceConnectorTopicPartitionOffsets(List<String> topicPartitions) {
        Map<String, Map<String, Object>> topicPartitionOffsets = new HashMap<>();
        String targetTopicPrefix = this.configMap.getOrDefault(AwsStorageSourceConnector.SINK_TOPIC_PREFIX, "");
        List<Map<String, String>> offsetPartitions = topicPartitions.stream().map(
                tp -> {
                    HashMap<String, String> offsetInfo = new HashMap<>();
                    offsetInfo.put("source", tp);
                    offsetInfo.put("targetPrefix", targetTopicPrefix);
                    return offsetInfo;
                }
        ).collect(Collectors.toList());
        Map<Map<String, String>, Map<String, Object>> offsetMap = context.offsetStorageReader().offsets(offsetPartitions);
        offsetMap.keySet().forEach(key -> {
            Map<String, Object> offset = offsetMap.get(key);
            topicPartitionOffsets.put(key.get("source"), offset);
        });
        return topicPartitionOffsets;
    }

    @Override
    public List<SourceRecord> poll() {
        if (System.currentTimeMillis() - lastPausedQueueScanTimeStamp >= pausedQueueScanIntervalMs) {
            awsSourceReader.refreshPausedAwsReadPositions();
            lastPausedQueueScanTimeStamp = System.currentTimeMillis();
        }
        String topicPartition = null;
        if(recordsToBeDelivered.isEmpty()) {
            try {
                TopicPartitionSegmentParser topicPartitionSegmentParser = awsSourceReader.getNextTopicPartitionSegmentParser();
                if (topicPartitionSegmentParser == null) {
                    Thread.sleep(pollSleepIntervalMs);
                    return null;
                }
                topicPartition = String.format("%s/%d", topicPartitionSegmentParser.getTopic(), topicPartitionSegmentParser.getPartition());
                long lastReadOffset = awsSourceReader.getLastReadOffset(topicPartition);
                boolean notComplete;
                do {
                    SourceRecord record = topicPartitionSegmentParser.getNextRecord(10L, TimeUnit.SECONDS);
                    if (record != null) {
                        long recordOffset = (Long) record.sourceOffset().get("lastReadOffset");
                        if (lastReadOffset <= recordOffset - 1) {
                            recordsToBeDelivered.add(record);
                            lastReadOffset = recordOffset;
                            awsSourceReader.setLastReadOffset(topicPartition, lastReadOffset);
                        } else if (lastReadOffset >= recordOffset) {
                            //duplicates
                            log.warn("Lower offset encountered when reading data. " +
                                    "Record Offset :  {}, " +
                                    "last submitted offset {}", recordOffset, lastReadOffset);
                        }
                        notComplete = recordOffset != topicPartitionSegmentParser.getEndOffset();
                    } else {
                        log.error("s3 object content stream closed before reaching end offset record : {}", topicPartitionSegmentParser.getEndOffset());
                        topicPartitionSegmentParser.closeResources();
                        throw new MissingRecordsException(String.format("Last successful committed record offset : %d , expected last record : %d",
                                lastReadOffset, topicPartitionSegmentParser.getEndOffset()));
                    }
                } while (notComplete);
                topicPartitionSegmentParser.closeResources();
            } catch (InterruptedException e) {
                log.info("Thread interrupted in poll. Shutting down", e);
                Thread.currentThread().interrupt();
            } catch (UncheckedExecutionException | ExecutionException | TimeoutException e) {
                Throwable exceptionCause = e.getCause();
                if (exceptionCause instanceof AmazonClientException) {
                    AmazonClientException amazonClientException = (AmazonClientException) exceptionCause;
                    if (!amazonClientException.isRetryable()) {
                        throw amazonClientException;
                    } else {
                        log.warn("Retryable S3 service exception while reading from s3", e);
                        if (topicPartition != null) {
                            awsSourceReader.revertAwsReadPositionMarker(topicPartition);
                        }
                    }
                } else if (exceptionCause instanceof IOException || e instanceof TimeoutException) {
                    log.warn("Retryable exception while reading from s3", e);
                    if (topicPartition != null) {
                        awsSourceReader.revertAwsReadPositionMarker(topicPartition);
                    }
                }
            } catch (RuntimeException e){
                throw e;
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        ArrayList<SourceRecord> sourceRecords = new ArrayList<>();
        while(!this.recordsToBeDelivered.isEmpty() && this.pollRecordRateLimiter.tryAcquire(this.pollSleepIntervalMs,TimeUnit.MILLISECONDS)){
            sourceRecords.add(this.recordsToBeDelivered.poll());
        }
        return sourceRecords;
    }


    @Override
    public void stop() {
        log.info("Task shutdown success");
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
