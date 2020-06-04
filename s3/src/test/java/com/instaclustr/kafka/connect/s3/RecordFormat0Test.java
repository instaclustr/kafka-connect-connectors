package com.instaclustr.kafka.connect.s3;

import com.instaclustr.kafka.connect.s3.sink.MaxBufferSizeExceededException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RecordFormat0Test {

    RecordFormat0 recordFormat;
    ByteArrayOutputStream bos;
    DataOutputStream dataOutputStream;

    @BeforeMethod
    public void setup() {
        recordFormat = new RecordFormat0();
        bos = new ByteArrayOutputStream();
        dataOutputStream = new DataOutputStream(bos);
    }

    @Test
    public void writeRecordNoHeaderTest() throws IOException, MaxBufferSizeExceededException {
        SinkRecord toWrite = new SinkRecord("blah", 0, Schema.BYTES_SCHEMA, new byte[]{1,2,3}, Schema.BYTES_SCHEMA, new byte[]{4,5,6,7}, 5, 17L, TimestampType.NO_TIMESTAMP_TYPE);

        int written = recordFormat.writeRecord(dataOutputStream, toWrite, 35);

        byte[] output = bos.toByteArray();

        Assert.assertEquals(output.length, written);
        Assert.assertEquals(output, new byte[]{
                0, 0, 0, 0, 0, 0, 0, 5 // offset
                , 0, 0, 0, 0, 0, 0, 0, 17 // timestamp
                , 0, 0, 0, 3 // key length
                , 0, 0, 0, 4 // value length
                , 0, 0, 0, 0 // header size
                , 1, 2, 3 // key
                , 4, 5, 6, 7 // value
        });
    }

    @Test
    public void tooLargeTest() throws IOException {
        SinkRecord toWrite = new SinkRecord("blah", 0, Schema.BYTES_SCHEMA, new byte[]{1,2,3}, Schema.BYTES_SCHEMA, new byte[]{4,5,6,7}, 5, 17L, TimestampType.NO_TIMESTAMP_TYPE);

        try{
            recordFormat.writeRecord(dataOutputStream, toWrite, 34);
            Assert.fail("expected exception");
        } catch (MaxBufferSizeExceededException ex) {
            //expected exception
        }
    }

    @Test
    public void writeEmptiesTest() throws IOException, MaxBufferSizeExceededException {
        SinkRecord toWrite = new SinkRecord("blah", 0, Schema.BYTES_SCHEMA, new byte[0], Schema.BYTES_SCHEMA, new byte[0], 5, 17L, TimestampType.NO_TIMESTAMP_TYPE);

        int written = recordFormat.writeRecord(dataOutputStream, toWrite, 35);

        byte[] output = bos.toByteArray();

        Assert.assertEquals(output.length, written);
        Assert.assertEquals(output, new byte[]{
                0, 0, 0, 0, 0, 0, 0, 5 // offset
                , 0, 0, 0, 0, 0, 0, 0, 17 // timestamp
                , 0, 0, 0, 0 // key length
                , 0, 0, 0, 0 // value length
                , 0, 0, 0, 0 // header size
        });
    }

    @Test
    public void writeNullsTest() throws IOException, MaxBufferSizeExceededException {
        SinkRecord toWrite = new SinkRecord("blah", 0, Schema.BYTES_SCHEMA, null, Schema.BYTES_SCHEMA, null, 4, 16L, TimestampType.NO_TIMESTAMP_TYPE);

        int written = recordFormat.writeRecord(dataOutputStream, toWrite, 35);

        byte[] output = bos.toByteArray();

        Assert.assertEquals(output.length, written);
        Assert.assertEquals(output, new byte[]{
                0, 0, 0, 0, 0, 0, 0, 4 // offset
                , 0, 0, 0, 0, 0, 0, 0, 16 // timestamp
                , -1, -1, -1, -1 // key length
                , -1, -1, -1, -1 // value length
                , 0, 0, 0, 0 // header size
        });
    }

    @Test
    public void readWriteWithHeadersTest() throws IOException, MaxBufferSizeExceededException {
        Headers headers = new ConnectHeaders();
        headers.addString("a", "asdas");
        headers.addString("b", "");
        headers.add("cc", null, null);
        List<Header> inHeaders = new ArrayList<>();
        for (Header h : headers) {
            inHeaders.add(h);
        }

        SinkRecord toWrite = new SinkRecord("blah", 0, Schema.BYTES_SCHEMA, new byte[]{1,2,3}, Schema.BYTES_SCHEMA, new byte[]{4,5,6,7}, 5, 17L, TimestampType.NO_TIMESTAMP_TYPE, headers);
        recordFormat.writeRecord(dataOutputStream, toWrite, 100);

        byte[] output = bos.toByteArray();
        DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(output));

        HashMap<String, Object> sourcePartition = new HashMap<>();
        sourcePartition.put("QQ", "WW");

        HashMap<String, Object> sourceOffsets = new HashMap<>();
        sourceOffsets.put("AA", "SS");

        SourceRecord sourceRecord = recordFormat.readRecord(dataInputStream, sourcePartition, sourceOffsets, "topic", 1);

        Assert.assertEquals(sourceRecord.sourceOffset().get("AA"), "SS");
        Assert.assertEquals(sourceRecord.sourcePartition().get("QQ"), "WW");
        Assert.assertEquals(sourceRecord.key(), new byte[]{1,2,3});
        Assert.assertEquals(sourceRecord.value(), new byte[]{4,5,6,7});
        List<Header> outHeaders = new ArrayList<>();
        for (Header h : sourceRecord.headers()) {
            outHeaders.add(h);
        }
        Assert.assertEquals(outHeaders, inHeaders);

    }

}
