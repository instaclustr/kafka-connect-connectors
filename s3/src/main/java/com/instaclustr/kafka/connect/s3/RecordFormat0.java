package com.instaclustr.kafka.connect.s3;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.instaclustr.kafka.connect.s3.sink.MaxBufferSizeExceededException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import com.google.gson.JsonParser;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;


public class RecordFormat0 implements RecordFormat {
    private static Logger logger = LoggerFactory.getLogger(RecordFormat0.class);
    private Gson gson = (new GsonBuilder()).serializeNulls().setLenient().create();
    private JsonParser jsonParser = new JsonParser();

    private byte[] lineSeparatorBytes = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

    public RecordFormat0() {
    }

    @Override
    public int writeRecord(final DataOutputStream dataOutputStream, final SinkRecord record, int sizeLimit) throws MaxBufferSizeExceededException, IOException {
        String keyData = (record.key() == null || Arrays.equals(((byte[]) record.key()), "".getBytes())) ? null : (asUTF8String((byte[]) record.key()));
        String valueData = (record.value() == null || Arrays.equals(((byte[]) record.value()), "".getBytes())) ? null : (asUTF8String((byte[]) record.value()));

        byte[] writableRecord = constructJson(new Record(keyData, valueData, record.timestamp(), record.kafkaOffset())).getBytes();
        int nextChunkSize = writableRecord.length + lineSeparatorBytes.length;

        if (nextChunkSize > sizeLimit) {
            throw new MaxBufferSizeExceededException();
        }

        dataOutputStream.write(writableRecord);
        dataOutputStream.write(lineSeparatorBytes);

        return nextChunkSize;
    }

    @Override
    public SourceRecord readRecord(final String jsonRow, final Map<String, ?> sourcePartition,
                                   final Map<String, Object> sourceOffset, final String topic, final int partition) throws IOException, NumberFormatException {

        try {
            JsonObject jsonObject = jsonParser.parse(jsonRow).getAsJsonObject();

            if (jsonObject.isJsonObject()) {
                byte[] key = (jsonObject.get("k").isJsonNull()) ? null : readAsObjectOrString(jsonObject, "k").getBytes();
                byte[] value = (jsonObject.get("v").isJsonNull()) ? null : readAsObjectOrString(jsonObject, "v").getBytes();
                long timestamp = jsonObject.get("t").getAsLong();
                long offset = jsonObject.get("o").getAsLong();

                sourceOffset.put("lastReadOffset", offset);
                return new SourceRecord(sourcePartition, sourceOffset, topic, partition, Schema.BYTES_SCHEMA, key, Schema.BYTES_SCHEMA, value, timestamp);
            } else {
                logger.error("Did not receive a json object " + jsonRow);
                throw new IOException("Did not receive a json object " + jsonRow);
            }
        } catch (Exception e) {
            logger.error("Could not construct Source Record, reason: " + e.getMessage());
            throw new IOException("Could not construct Source Record, reason: " + e.getMessage());
        }
    }

    private String constructJson(Record record) {
        return gson.toJson(record);
    }

    private String readAsObjectOrString(JsonObject jsonObject, String memberName) {
        try {
            return jsonObject.getAsJsonObject(memberName).toString();
        } catch (ClassCastException e) {
            return jsonObject.get(memberName).getAsString();
        }
    }

    private String asUTF8String(byte[] ba) {
        return new String(ba, StandardCharsets.UTF_8);
    }
}
