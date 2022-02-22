package com.instaclustr.kafka.connect.s3.source;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.instaclustr.kafka.connect.s3.AwsConnectorStringFormats;
import com.instaclustr.kafka.connect.s3.RecordFormat;
import com.instaclustr.kafka.connect.s3.RecordFormat0;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;

/**
 * This class handles converting S3Objects into SourceRecords using the relevant RecordFormat
 * This class is used in order to read the records within each segment (topic/partition/startOffset-endOffset.txt).
 * Foreach such segment we are loading all the records split by '\n' as an Iterator, constructing each record as a SourceRecord.
 */

public class TopicPartitionSegmentParser implements Iterator<String> {
    private final String targetTopic;
    private BufferedReader bufferedReader;
    private Iterator<String> lines;
    private TimeLimiter timeLimiter;
    private final String topic;
    private String s3ObjectKey;
    private String topicPrefix;
    private final int partition;
    private final long startOffset;
    private final long endOffset;
    private RecordFormat recordFormat;
    private ExecutorService singleThreadExecutor;

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public TopicPartitionSegmentParser(final InputStream s3ObjectInputStream, final String s3ObjectKey, final String topicPrefix) {
        Matcher fileNameMatcher = AwsConnectorStringFormats.S3_OBJECT_KEY_PATTERN.matcher(s3ObjectKey);
        if (fileNameMatcher.matches()) {
            this.topic = fileNameMatcher.group(1);
            this.partition = Integer.parseInt(fileNameMatcher.group(2));
            this.startOffset = Long.parseLong(fileNameMatcher.group(3));
            this.endOffset = Long.parseLong(fileNameMatcher.group(4));
        } else {
            throw new IllegalArgumentException("filename is not in a valid format");
        }
        this.bufferedReader = new BufferedReader(new InputStreamReader(s3ObjectInputStream, StandardCharsets.UTF_8));
        this.lines = this.bufferedReader.lines().iterator();
        this.s3ObjectKey = s3ObjectKey;
        this.topicPrefix = topicPrefix;
        this.targetTopic = AwsConnectorStringFormats.generateTargetTopic(topicPrefix, topic);
        this.singleThreadExecutor = Executors.newSingleThreadExecutor();
        this.timeLimiter = SimpleTimeLimiter.create(this.singleThreadExecutor);
    }

    public void closeResources() throws IOException, InterruptedException {
        bufferedReader.close();
        this.singleThreadExecutor.shutdownNow();
        this.singleThreadExecutor.awaitTermination(5L, TimeUnit.SECONDS);
    }

    private SourceRecord getNextRecord() throws IOException { //blocking call
        try {
            if (recordFormat == null) {
                if (bufferedReader.ready()) {
                    recordFormat = new RecordFormat0();
                } else {
                    throw new IOException("Unknown version format");
                }
            }

            HashMap<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put("source", String.format("%s/%d", this.topic, this.partition));
            sourcePartition.put("targetPrefix", this.topicPrefix);

            HashMap<String, Object> sourceOffset = new HashMap<>();
            sourceOffset.put("startOffset", AwsConnectorStringFormats.convertLongIntoLexySortableString(this.startOffset));
            sourceOffset.put("endOffset", AwsConnectorStringFormats.convertLongIntoLexySortableString(this.endOffset));
            sourceOffset.put("s3ObjectKey", s3ObjectKey);

            return recordFormat.readRecord(next(), sourcePartition, sourceOffset, this.targetTopic, this.partition);
        } catch (EOFException | NoSuchElementException e) {
            return null;
        }
    }

    public SourceRecord getNextRecord(Long time, TimeUnit units) throws Exception {
        try {
            return this.timeLimiter.callWithTimeout(this::getNextRecord, time, units);
        } catch (Exception e) {
            this.closeResources(); //not possible to read from this stream after a timeout as read positions gets messed up
            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        return lines.hasNext();
    }

    @Override
    public String next() {
        if (!lines.hasNext()) {
            throw new NoSuchElementException("Reach the end of the Source Data");
        }
        return lines.next().trim();
    }
}
