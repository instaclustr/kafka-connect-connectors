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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RecordFormat0 implements RecordFormat {
    public static final int RECORD_METADATA_LENGTH = Integer.BYTES * 3 + Long.BYTES * 2; //key, value, header length + offset length + timestamp

    private ByteArrayConverter byteArrayConverter;
    private SimpleHeaderConverter headerConverter;


    public RecordFormat0() {
        byteArrayConverter = new ByteArrayConverter();
        headerConverter = new SimpleHeaderConverter();
    }

    @Override
    public int writeRecord(final DataOutputStream dataOutputStream, final SinkRecord record, int sizeLimit) throws MaxBufferSizeExceededException, IOException {
        byte[] keyData = byteArrayConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
        byte[] valueData = byteArrayConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        List<Pair<byte[], byte[]>> headerData = new LinkedList<>();

        int headerDataTotalLength = 0;
        if (record.headers() != null) {
            for (Header header : record.headers()) {
                ImmutablePair<byte[], byte[]> headerBytesPair = new ImmutablePair<>(
                        header.key().getBytes(UTF_8),
                        this.headerConverter.fromConnectHeader(record.topic(), header.key(), header.schema(), header.value()));
                if (headerBytesPair.right != null) {
                    headerDataTotalLength += headerBytesPair.left.length + Integer.BYTES * 2 + headerBytesPair.right.length;
                } else {
                    headerDataTotalLength += headerBytesPair.left.length + Integer.BYTES * 2;
                }
                headerData.add(headerBytesPair);
            }
        }

        int keyLength = (keyData == null) ? -1 : keyData.length;
        int valueLength = (valueData == null) ? -1 : valueData.length;

        if (keyData == null) keyData = new byte[0];
        if (valueData == null) valueData = new byte[0];

        int nextChunkSize = RECORD_METADATA_LENGTH + keyData.length + valueData.length + headerDataTotalLength;

        if (nextChunkSize > sizeLimit) {
            throw new MaxBufferSizeExceededException();
        }

        dataOutputStream.writeLong(record.kafkaOffset());
        dataOutputStream.writeLong(record.timestamp());
        dataOutputStream.writeInt(keyLength);
        dataOutputStream.writeInt(valueLength);
        dataOutputStream.writeInt(headerData.size());
        dataOutputStream.write(keyData);
        dataOutputStream.write(valueData);
        for (Pair<byte[], byte[]> headerDataPair : headerData) {
            dataOutputStream.writeInt(headerDataPair.getLeft().length); //key
            dataOutputStream.write(headerDataPair.getLeft());
            if (headerDataPair.getRight() != null) {
                dataOutputStream.writeInt(headerDataPair.getRight().length); //value
                dataOutputStream.write(headerDataPair.getRight());
            } else {
                dataOutputStream.writeInt(-1);
            }
        }

        return nextChunkSize;
    }

    @Override
    public SourceRecord readRecord(final DataInputStream dataInputStream, final Map<String, ?> sourcePartition, final Map<String, Object> sourceOffset, final String topic, final int partition) throws IOException {
        long offset = dataInputStream.readLong();
        long timestamp = dataInputStream.readLong();
        int keyLength = dataInputStream.readInt();
        int valueLength = dataInputStream.readInt();
        int headersCount = dataInputStream.readInt();

        byte[] keyHolder;
        byte[] valueHolder;
        if (keyLength == -1) {
            keyHolder = null;
        } else {
            keyHolder = dataInputStream.readNBytes(keyLength);
        }
        if (valueLength == -1) {
            valueHolder = null;
        } else {
            valueHolder = dataInputStream.readNBytes(valueLength);
        }
        Headers headers = new ConnectHeaders();

        for (int i = 0; i < headersCount; i++) {
            int headerKeyLength = dataInputStream.readInt();
            String key = new String(dataInputStream.readNBytes(headerKeyLength), UTF_8);
            int headerValueLength = dataInputStream.readInt();
            if (headerValueLength == -1) {
                headers.add(key, this.headerConverter.toConnectHeader(topic, key, null));
            } else {
                headers.add(key, this.headerConverter.toConnectHeader(topic, key, dataInputStream.readNBytes(headerValueLength)));
            }
        }

        sourceOffset.put("lastReadOffset", offset);
        return new SourceRecord(sourcePartition, sourceOffset, topic, partition, Schema.BYTES_SCHEMA, keyHolder, Schema.BYTES_SCHEMA, valueHolder, timestamp, headers);
    }
}
