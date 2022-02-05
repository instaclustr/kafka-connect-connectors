package com.instaclustr.kafka.connect.s3;

import com.instaclustr.kafka.connect.s3.sink.MaxBufferSizeExceededException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RecordFormat0 implements RecordFormat {
    private byte[] lineSeparatorBytes = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

    public RecordFormat0() {}

    @Override
    public int writeRecord(final DataOutputStream dataOutputStream, final SinkRecord record, int sizeLimit) throws MaxBufferSizeExceededException, IOException {
        byte[] keyData = (byte[]) record.key();
        byte[] valueData = (byte[]) record.value();

        byte[] keyLength = ((keyData == null) ? "0" : Long.toString(keyData.length)).getBytes();
        byte[] valueLength = ((valueData == null) ? "0" : Long.toString(valueData.length)).getBytes();

        if (keyData == null) keyData = new byte[0];
        if (valueData == null) valueData = new byte[0];

        byte[] kafkaOffset = Long.toString(record.kafkaOffset()).getBytes();
        byte[] timestamp = Long.toString(record.timestamp()).getBytes();

        int nextChunkSize = kafkaOffset.length + timestamp.length + keyLength.length + valueLength.length +
                keyData.length + valueData.length + lineSeparatorBytes.length;

        if (nextChunkSize > sizeLimit) {
            throw new MaxBufferSizeExceededException();
        }

        // check writing as UTF and able to read it back
        dataOutputStream.write(kafkaOffset);
        dataOutputStream.write(timestamp);
        dataOutputStream.write(keyLength);
        dataOutputStream.write(valueLength);
        dataOutputStream.write(keyData);
        dataOutputStream.write(valueData);
        dataOutputStream.write(lineSeparatorBytes);
//        dataOutputStream.flush();

        // offset, timestamp, key, value
        return nextChunkSize;
    }

    @Override
    public SourceRecord readRecord(final DataInputStream dataInputStream, final Map<String, ?> sourcePartition,
                                   final Map<String, Object> sourceOffset, final String topic, final int partition) throws IOException, NumberFormatException {

        long offset = Long.parseLong(dataInputStream.readUTF());
        long timestamp = Long.parseLong(dataInputStream.readUTF());
        int keyLength = Integer.parseInt(dataInputStream.readUTF());
        int valueLength = Integer.parseInt(dataInputStream.readUTF());

        System.out.println(">>>>>> offset: " + offset);
        System.out.println(">>>>>> timestamp: " + timestamp);
        System.out.println(">>>>>> keyLength: " + keyLength);
        System.out.println(">>>>>> valueLength: " + valueLength);

        byte[] keyHolder;
        byte[] valueHolder;
        if (keyLength == 0) {
            keyHolder = null;
        } else {
            keyHolder = dataInputStream.readNBytes(keyLength);
        }
        if (valueLength == 0) {
            valueHolder = null;
        } else {
            valueHolder = dataInputStream.readNBytes(valueLength);
        }

        sourceOffset.put("lastReadOffset", offset);
        return new SourceRecord(sourcePartition, sourceOffset, topic, partition, Schema.STRING_SCHEMA, keyHolder, Schema.STRING_SCHEMA, valueHolder, timestamp);
    }
}
