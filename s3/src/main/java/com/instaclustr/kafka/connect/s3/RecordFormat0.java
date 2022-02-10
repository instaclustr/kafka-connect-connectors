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
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;


public class RecordFormat0 implements RecordFormat {
    private static Logger logger = LoggerFactory.getLogger(RecordFormat0.class);

    private byte[] lineSeparatorBytes = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

    public RecordFormat0() {
    }

    @Override
    public int writeRecord(final DataOutputStream dataOutputStream, final SinkRecord record, int sizeLimit) throws MaxBufferSizeExceededException, IOException {
        byte[] keyData = (record.key() == null || Arrays.equals(((byte[]) record.key()), "".getBytes())) ? "null".getBytes() : ((byte[]) record.key());
        byte[] valueData = (record.value() == null || Arrays.equals(((byte[]) record.value()), "".getBytes())) ? "null".getBytes() : ((byte[]) record.value());

        String recordStr = recordAsJson(asString(keyData), asString(valueData), record.timestamp(), record.kafkaOffset());
        logger.debug(">>>>>>Writing record: " + recordStr);
        byte[] writableRecord = recordStr.getBytes();

        int nextChunkSize = writableRecord.length + lineSeparatorBytes.length;

        if (nextChunkSize > sizeLimit) {
            throw new MaxBufferSizeExceededException();
        }

        dataOutputStream.write(writableRecord);
        dataOutputStream.write(lineSeparatorBytes);
//        dataOutputStream.flush();

        return nextChunkSize;
    }

    @Override
    public SourceRecord readRecord(final String singleRow, final Map<String, ?> sourcePartition,
                                   final Map<String, Object> sourceOffset, final String topic, final int partition) throws IOException, NumberFormatException {
        logger.debug(">>>>>>Reading record: " + singleRow);

        JSONObject jsonObject = new JSONObject(singleRow);
        byte[] key = (jsonObject.isNull("k")) ? null : jsonObject.getJSONObject("k").toString().getBytes();
        byte[] value = (jsonObject.isNull("v")) ? null : jsonObject.getJSONObject("v").toString().getBytes();
        long timestamp = jsonObject.getLong("t");
        long offset = jsonObject.getLong("o");

        sourceOffset.put("lastReadOffset", offset);
        return new SourceRecord(sourcePartition, sourceOffset, topic, partition, Schema.BYTES_SCHEMA, key, Schema.BYTES_SCHEMA, value, timestamp);
    }

    private String recordAsJson(Object key, Object value, long timestamp, long offset) {
        return "{"
                + "\"k\":" + key + ","
                + "\"v\":" + value + ","
                + "\"t\":" + timestamp + ","
                + "\"o\":" + offset +
                "}";
    }

    private String asString(byte[] ba) {
        return new String(ba, StandardCharsets.UTF_8);
    }
}
