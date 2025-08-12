package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.codec.Decoder;
import com.instaclustr.kafka.connect.stream.codec.Decoders;
import com.instaclustr.kafka.connect.stream.codec.Record;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class StreamSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(StreamSourceTask.class);

    public static final String TASK_FILES = "task.files";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";
    public static final String TOPIC_CONFIG = "topic";
    public static final String POLL_THROTTLE_MS = "poll.throttle.ms";
    public static final String READ_RETRIES = "read.retries";
    
    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;
    public static final int DEFAULT_READ_RETRIES = 10;
    public static final long DEFAULT_POLL_THROTTLE_MS = 1000;

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TASK_FILES,
                    ConfigDef.Type.LIST,
                    List.of(),
                    new ConfigDef.NonNullValidator(),
                    ConfigDef.Importance.HIGH,
                    "Files assigned to this task by the connector")
            .define(TASK_BATCH_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    DEFAULT_TASK_BATCH_SIZE,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.LOW,
                    "The maximum number of records the source task can read from the file each time it is polled")
            .define(TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyStringWithoutControlChars(),
                    ConfigDef.Importance.HIGH,
                    "The topic to publish data to")
            .define(READ_RETRIES,
                    ConfigDef.Type.INT,
                    DEFAULT_READ_RETRIES,
                    ConfigDef.Range.atLeast(0),
                    ConfigDef.Importance.LOW,
                    "The maximum number of retries on reading a stream")
            .define(POLL_THROTTLE_MS,
                    ConfigDef.Type.LONG,
                    DEFAULT_POLL_THROTTLE_MS,
                    ConfigDef.Range.atLeast(0),
                    ConfigDef.Importance.LOW,
                    "The time to wait for throttle source polling");
    
    public static final String FILENAME_FIELD = "filename";
    public static final String POSITION_FIELD = "position";
    public static final String PROGRESS_FIELD = "progress";

    private String topic;
    private int batchSize;
    private int maxReadRetries;
    private long pollThrottleMs;
    private Map<String, String> props;

    private Endpoint endpoint;
    
    // Mutable state
    private Queue<String> filenames; // head of the queue is the current file
    private Decoder<?> decoder = null;
    private int numTries = 0;

    // For Connect runtime to load this class
    public StreamSourceTask() {
    }

    @Override
    public String version() {
        return new StreamSourceConnector().version();
    }

    @Override
    public void start(final Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);

        endpoint = Endpoints.of(props);
        topic = config.getString(TOPIC_CONFIG);
        batchSize = config.getInt(TASK_BATCH_SIZE_CONFIG);
        maxReadRetries = config.getInt(READ_RETRIES);
        pollThrottleMs = config.getLong(POLL_THROTTLE_MS);

        // Initialize task mutable state
        filenames = new LinkedList<>(config.getList(TASK_FILES));
        if (filenames.isEmpty()) {
            throw new ConnectException(
                    "Unable to find files to read: " + props.get(TASK_FILES));
        }
        // Set decoder to null so poll() opens a fresh stream
        decoder = null;
        numTries = 0;

        this.props = props;

        log.info("Started task reading from {} for topic {}", filenames, topic);
    }

    /**
     * Poll the stream source for new records.
     * 
     * @return null if blocking for more source input; a nonempty list of records
     *         otherwise; never an empty list.
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Process a list of files one by one
        // For each file,
        // - set filename, topic, and its decoder (open stream and seek)
        // - reading until EOF or not ready
        // - when EOF or not ready, pause on this file by closing its stream, process
        // next file
        // - keep looping forever over all files
        // - what happens when error on one file? move this file to the end the queue and move on
        if (decoder == null) {
            decoder = maybeGetNextFileDecoder();
            if (decoder == null) {
                log.info("No available file to read, will try again later");
                waitForThrottle();
                return null;
            }
        }

        try {
            String filename = filenames.element();
            List<? extends Record<?>> batch = decoder.next(batchSize);
            if (batch == null) {
                numTries++;
                log.info("Stream is not available to read, at try: {}, file: {}", numTries, filename);
                if (numTries > maxReadRetries) { // 1 + retries = total number of tries
                    log.info("Reached retry limit: {}, tries: {}, file: {}", maxReadRetries, numTries, filename);
                    closeForNextFile();
                } else if (isEof(filename)) {
                    log.info("Continual reads reached EOF: {}", filename);
                    closeForNextFile();
                }
                waitForThrottle();
                return null;
            }

            if (batch.isEmpty()) {
                log.debug("Read some new bytes from stream, but not enough yet to decode any record");
                // Not counting towards a retry
                return null;
            }

            List<SourceRecord> records = new ArrayList<>();
            for (var r : batch) {
                records.add(new SourceRecord(
                            offsetKey(filename), 
                            offsetValue(r.getStreamOffset(), r.getStreamProgress()), 
                            topic,
                            null, 
                            null, 
                            null, 
                            r.getSchema(), 
                            r.getRecord(), 
                            System.currentTimeMillis()));
            }
            log.debug("Return records after decoding or waiting, size: {}, tries: {}", records.size(), numTries);
            numTries = 0; // Next try is first try
            // Non-empty result for Connect runtime to work on; otherwise null returns above
            // indicate no result
            return records;
        } catch (IOException e) {
            log.error("Error in reading records", e);
            closeForNextFile();
            waitForThrottle();
        }
        return null;
    }

    private Decoder<?> maybeGetNextFileDecoder() {
        log.info("Looking for next file to read, total files: {}", filenames.size());
        Decoder<?> result = null;
        for (int i = 0; i < filenames.size() && result == null; i++) {
            String filename = filenames.element();

            Map<String, Object> state = context.offsetStorageReader()
                    .offset(Collections.singletonMap(FILENAME_FIELD, filename));
            log.debug("Read offset: {}, thread: {}, file: {}", state, Thread.currentThread().getName(), filename);
            Optional<Long> lastReadOffset = getLastReadOffset(state);
            Optional<Double> lastReadProgress = getLastReadProgress(state);

            try {
                if (isEofByOffset(lastReadOffset, filename) || isEofByProgress(lastReadProgress)) {
                    log.info("Skip opening stream, last read was at end of file: {}", filename);
                    closeForNextFile();
                    continue;
                }

                result = Decoders.of(endpoint, filename, props);

                if (lastReadOffset.isPresent()) {
                    result.skipFirstBytes(lastReadOffset.get());
                    log.info("Skipped to offset: {}, file: {}", lastReadOffset.get(), filename);
                }
                log.info("Opened {} for reading", filename);
            } catch (IOException e) {
                log.error("Error while trying to open stream {}: ", filename, e);
                closeForNextFile(); // Set decoder to null
            }
        }

        return result;
    }

    private boolean isEof(String filename) throws IOException {
        Map<String, Object> state = context.offsetStorageReader()
                .offset(Collections.singletonMap(FILENAME_FIELD, filename));
        Optional<Long> lastReadOffset = getLastReadOffset(state);
        Optional<Double> lastReadProgress = getLastReadProgress(state);
        return isEofByOffset(lastReadOffset, filename) || isEofByProgress(lastReadProgress);
    }

    private boolean isEofByOffset(Optional<Long> lastReadOffset, String filename) throws IOException {
        // Get file size lazily due to I/O.  This method might be called periodically on closed files
        return lastReadOffset.isPresent() && lastReadOffset.get() >= endpoint.getFileSize(filename);
    }

    private boolean isEofByProgress(Optional<Double> lastReadProgress) {
        return lastReadProgress.isPresent() && lastReadProgress.get() >= 1.0;
    }

    private Optional<Long> getLastReadOffset(Map<String, Object> offset) {
        Optional<Long> result = Optional.empty();
        if (offset != null) {
            Object lastRecordedOffset = offset.get(POSITION_FIELD);
            if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                throw new ConnectException("Offset position is the incorrect type");
            if (lastRecordedOffset != null) {
                log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                result = Optional.of((Long) lastRecordedOffset);
            }
        }
        return result;
    }

    private Optional<Double> getLastReadProgress(Map<String, Object> offset) {
        Optional<Double> result = Optional.empty();
        if (offset != null) {
            Object lastRecordedProgress = offset.get(PROGRESS_FIELD);
            if (lastRecordedProgress != null && !(lastRecordedProgress instanceof Double))
                throw new ConnectException("Offset progress is the incorrect type: " + lastRecordedProgress.getClass().getName());
            if (lastRecordedProgress != null) {
                log.debug("Found previous offset progress: {}", lastRecordedProgress);
                result = Optional.of((Double) lastRecordedProgress);
            }
        }
        return result;
    }

    private void waitForThrottle() throws InterruptedException {
        synchronized (this) {
            log.debug("Waiting for poll throttle: {} ms", pollThrottleMs);
            this.wait(pollThrottleMs);
        }
    }

    private void closeForNextFile() {
        log.info("Preparing for next file after closing file: {}", filenames.peek());
        // Move file to last in list to read, reset decoder
        maybeCloseDecoder();
        decoder = null;
        filenames.add(filenames.remove());
        numTries = 0;
    }

    @Override
    public void stop() {
        log.info("Stopping");
        synchronized (this) {
            maybeCloseDecoder();
            // Don't set decoder to null, because prior to Kafka 2.8, stop() and poll() can
            // be called
            // on separate threads, leading to race condition on decoder.
            // However, not setting decoder to null after stop() may lead to stale poll().
            // So, set decoder to null in start() to ensure restarting a task gets fresh
            // stream.

            // Wake up any waiting poll() to finish
            // Can live with chance of having one waiting poll() AFTER stop
            this.notify();
        }
    }

    private void maybeCloseDecoder() {
        try {
            if (decoder != null) {
                decoder.close();
                log.debug("Closed stream reader");
            }
        } catch (IOException e) {
            log.error("Failed to close stream reader: ", e);
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Object> offsetValue(Long pos, Double progress) {
        Map<String, Object> result = new HashMap<>();
        result.put(POSITION_FIELD, pos);
        result.put(PROGRESS_FIELD, progress);
        return result;
    }

    /* Visible for testing */
    void setEndpoint(final Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public Set<String> getFilenames() {
        return Set.copyOf(filenames);
    }
}
