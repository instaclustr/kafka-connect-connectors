package com.instaclustr.kafka.connect.s3.sink;

import com.instaclustr.kafka.connect.s3.RecordFormat;
import com.instaclustr.kafka.connect.s3.RecordFormat0;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * The fixed capacity buffer used to store SinkRecords given to the sink tasks from kafka connect.
 * The SinkRecords are constructed as json values which are then encoded into bytes.
 * Each json record is followed by the '\n' character.
 */

public class TopicPartitionBuffer {
    public final String topic;
    public final int partition;
    private long startOffset;
    private long endOffset;
    private final int capacity;
    private int currentPosition;
    private DataOutputStream dataStream;
    private PipedInputStream pipedInputStream;
    private RecordFormat recordFormat;

    private static Logger logger = LoggerFactory.getLogger(TopicPartitionBuffer.class);

    public TopicPartitionBuffer(final TopicPartition topicPartition) throws IOException {
        this(topicPartition.topic(), topicPartition.partition());
    }

    public TopicPartitionBuffer(final String topic, final int partition) throws IOException {
        this(topic, partition, 3 * 1024 * 1024); // default max is 3MB
    }

    public TopicPartitionBuffer(final String topic, final int partition, int maxSizeBytes) throws IOException {
        this.topic = topic;
        this.partition = partition;
        this.capacity = maxSizeBytes;
        this.pipedInputStream = new PipedInputStream(this.capacity);
        this.dataStream = new DataOutputStream(new PipedOutputStream(this.pipedInputStream));
        this.currentPosition = 0;
        this.startOffset = -1;
        this.endOffset = -1;
        this.recordFormat = new RecordFormat0();
    }

    public void putRecord(SinkRecord record) throws IOException, RecordOutOfOrderException, MaxBufferSizeExceededException {
        if (endOffset >= record.kafkaOffset()) {
            throw new RecordOutOfOrderException("Record offset must always be larger than the buffer latest record offset");
        }

        if (startOffset == -1) {
            startOffset = record.kafkaOffset();
        }

        currentPosition += recordFormat.writeRecord(dataStream, record, capacity - currentPosition);
        endOffset = record.kafkaOffset();
    }

    public void flush() throws IOException {
        dataStream.flush();
    }

    // Use once only
    public InputStream getInputStream() throws IOException {
        this.flush();
        return pipedInputStream;
    }

    public void cleanResources() {
        try {
            this.dataStream.close();
            this.pipedInputStream.close();
            this.pipedInputStream = null;
            this.dataStream = null;
        } catch (IOException e) {
            logger.error("Failed cleaning up resources for buffer {} {}", this.topic, this.partition);
        }
    }

    public int getInputStreamLength() {
        return this.currentPosition;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public TopicPartition toTopicPartition() {
        return new TopicPartition(topic, partition);
    }

}
