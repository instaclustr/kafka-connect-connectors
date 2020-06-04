package com.instaclustr.kafka.connect.s3.source;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.instaclustr.kafka.connect.s3.AwsConnectorStringFormats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * This class handles reading S3Objects for the assigned topic partitions and keeping track of the read position
 * It will return a TopicPartitionSegmentParser per valid S3Object read in
 *
 * Any Aws sdk call may throw an AmazonS3Exception, AmazonServiceException or AmazonClientException
 * https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/java-dg-exceptions.html
 */

public class AwsSourceReader {
    private AmazonS3 s3Client;
    private LinkedList<AwsReadPosition> pausedReadPositions;
    private LinkedList<AwsReadPosition> activeReadPositions;
    private final String awsS3CommonPrefix;
    private final String awsBucket;
    private final String topicPrefix;
    private HashMap<String, AwsReadPosition> topicPartitionReadPositions;
    private static final String MARKER_TEMPLATE = "%s%s-%s";

    private static Logger log = LoggerFactory.getLogger(AwsSourceReader.class);

    public AwsSourceReader(final AmazonS3 s3Client, final String bucket, final String awsPrefix, String topicPrefix, Map<String, Map<String, Object>> topicPartitionOffsets) {
        this.s3Client = s3Client;
        this.awsS3CommonPrefix = awsPrefix;
        this.topicPrefix = topicPrefix;
        this.awsBucket = bucket;
        this.topicPartitionReadPositions = generateReadPositionsForEachTopicPartition(topicPartitionOffsets);
        log.debug("Topic Partition read position size : {}",this.topicPartitionReadPositions.size());
        this.activeReadPositions = new LinkedList<>(topicPartitionReadPositions.values());
        this.pausedReadPositions = new LinkedList<>();
    }

    public TopicPartitionSegmentParser getNextTopicPartitionSegmentParser() {
        AwsReadPosition readPosition = this.activeReadPositions.peek();
        if (readPosition == null) {
            return null;
        }
        S3Object s3Object = null;
        Iterator<S3ObjectSummary> s3ObjectSummaryIterator = readPosition.getCurrentIterator();
        if (s3ObjectSummaryIterator.hasNext()) {
            S3ObjectSummary s3ObjectSummary = s3ObjectSummaryIterator.next();
            GetObjectRequest getObjectRequest = new GetObjectRequest(
                    s3ObjectSummary.getBucketName(),
                    s3ObjectSummary.getKey()
            );
            s3Object = s3Client.getObject(getObjectRequest);
            if (s3ObjectSummaryIterator.hasNext()) {
                activeReadPositions.add(activeReadPositions.poll());
            } else {
                pausedReadPositions.add(activeReadPositions.poll());
            }
            readPosition.setLastReadMarker(s3Object.getKey());
        } else {
            pausedReadPositions.add(activeReadPositions.poll());
        }
        return s3Object == null ? null : new TopicPartitionSegmentParser(s3Object.getObjectContent(), s3Object.getKey(), this.topicPrefix);
    }

    private HashMap<String, AwsReadPosition> generateReadPositionsForEachTopicPartition(Map<String, Map<String, Object>> topicPartitionOffsets) {
        HashMap<String, AwsReadPosition> awsReadPositions = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> topicPartitionOffsetEntry : topicPartitionOffsets.entrySet()) {
            Map<String, Object> sourceOffsetRecord;
            String awsS3Prefix = String.format("%s%s%s", awsS3CommonPrefix, topicPartitionOffsetEntry.getKey(), AwsConnectorStringFormats.AWS_S3_DELIMITER);
            S3Objects s3Objects = S3Objects.withPrefix(this.s3Client, awsBucket, awsS3Prefix);
            long lastRecordedOffset = -1; //lower than zero
            if ((sourceOffsetRecord = topicPartitionOffsetEntry.getValue()) != null) {
                String s3ObjectKey = (String) sourceOffsetRecord.get("s3ObjectKey");
                long retrievedLastRecordedOffset = (Long) sourceOffsetRecord.get("lastReadOffset");
                String lastRecordOffsetAsString = AwsConnectorStringFormats.convertLongIntoLexySortableString(retrievedLastRecordedOffset);
                boolean completedParsingAllRecords = lastRecordOffsetAsString.equals(sourceOffsetRecord.get("endOffset"));

                if (s3ObjectKey.startsWith(awsS3Prefix)) {
                    if (completedParsingAllRecords) {
                        s3Objects.withMarker(s3ObjectKey);
                    } else {
                        s3Objects.withMarker(String.format(MARKER_TEMPLATE, awsS3Prefix, sourceOffsetRecord.get("startOffset"), lastRecordOffsetAsString));
                    }
                    lastRecordedOffset = retrievedLastRecordedOffset;
                } else {
                    log.warn("Existing source offset record contains a different s3 prefix! Starting from the beginning");
                }
            }
            AwsReadPosition position = new AwsReadPosition(s3Objects, lastRecordedOffset);
            awsReadPositions.put(topicPartitionOffsetEntry.getKey(), position);
        }
        return awsReadPositions;
    }

    public void revertAwsReadPositionMarker(String topicPartition) {
        AwsReadPosition position = this.topicPartitionReadPositions.get(topicPartition);
        if (position.getLastReadOffset() > -1) {
            Matcher fileNameMatcher = AwsConnectorStringFormats.S3_OBJECT_KEY_PATTERN.matcher(position.getLastReadMarker());
            long startOffset;
            if (fileNameMatcher.matches()) {
                startOffset = Long.parseLong(fileNameMatcher.group(3));
            } else {
                throw new IllegalArgumentException(String.format("Last read marker is not in a valid format. %s", position.getLastReadMarker()));
            }
            position.renewIterator(String.format(MARKER_TEMPLATE, position.s3Objects.getPrefix(),
                    AwsConnectorStringFormats.convertLongIntoLexySortableString(startOffset),
                    AwsConnectorStringFormats.convertLongIntoLexySortableString(position.getLastReadOffset())));
        } else {
            position.renewIterator(position.getLastReadMarker());
        }
    }

    public void setLastReadOffset(String topicPartition, long offset){
        this.topicPartitionReadPositions.get(topicPartition).setLastReadOffset(offset);
    }

    public long getLastReadOffset(String topicPartition){
        return this.topicPartitionReadPositions.get(topicPartition).getLastReadOffset();
    }

    public void refreshPausedAwsReadPositions() {
        int currentSize = this.pausedReadPositions.size();
        for (int i = 0; i < currentSize; i++) {
            AwsReadPosition pausedPosition = this.pausedReadPositions.peek();
            pausedPosition.renewIterator(pausedPosition.getLastReadMarker());
            if (pausedPosition.getCurrentIterator().hasNext()) {
                this.activeReadPositions.add(this.pausedReadPositions.poll());
            } else {
                this.pausedReadPositions.add(this.pausedReadPositions.poll());
            }
        }
        log.debug("Current paused position size : {}",this.pausedReadPositions.size());
    }

}
