package com.instaclustr.kafka.connect.sla;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;

public class SlaSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(SlaSinkTask.class);

    private String filename;

    public SlaSinkTask() { //kc usage
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(SlaSinkConnector.FILE_CONFIG);
        log.info("Starting SlaSinkConnector, writing to {}", filename);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        long now = System.currentTimeMillis();
        try {
            final PrintStream outputStream = new PrintStream(
                    Files.newOutputStream(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    false,
                    StandardCharsets.UTF_8.name());

            for (SinkRecord record : sinkRecords) {
                long latency = now - record.timestamp();
                outputStream.println(latency);
            }

            outputStream.close();
        } catch (IOException e) {
            throw new ConnectException("Couldn't find or create file '" + filename + "' for SlaSinkTask", e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Nothing to do - flushing happens on every put
    }

    @Override
    public void stop() {
        log.info("Stopping SlaSinkConnector");
    }

}
