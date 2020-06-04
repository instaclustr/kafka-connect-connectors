package com.instaclustr.kafka.connect.s3;

import com.instaclustr.kafka.connect.s3.sink.MaxBufferSizeExceededException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Interface to define read/write format the connectors will use to read/write from s3 objects
 */

public interface RecordFormat {
    // Returns the number of bytes written
    // Throws a MaxBufferSizeExceededException if the record will be larger than the given sizeLimit
    int writeRecord(final DataOutputStream dataOutputStream, SinkRecord record, int sizeLimit) throws MaxBufferSizeExceededException, IOException;

    SourceRecord readRecord(final DataInputStream dataInputStream, final Map<String, ?> sourcePartition, final Map<String, Object> sourceOffset, final String topic, final int partition) throws IOException;

}
