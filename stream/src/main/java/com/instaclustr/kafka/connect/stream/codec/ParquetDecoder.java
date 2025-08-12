package com.instaclustr.kafka.connect.stream.codec;

import com.instaclustr.kafka.connect.stream.RandomAccessInputStream;
import com.instaclustr.kafka.connect.stream.types.parquet.ParquetKafkaDataConverter;
import com.instaclustr.kafka.connect.stream.types.parquet.StreamInputFile;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.StreamParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetDecoder implements Decoder<Struct> {
    private static final Logger log = LoggerFactory.getLogger(ParquetDecoder.class);
    private final StreamParquetReader reader;
    private final ParquetKafkaDataConverter converter;

    public ParquetDecoder(StreamParquetReader reader, ParquetKafkaDataConverter converter) {
        this.reader = reader;
        this.converter = converter;
    }

    public static ParquetDecoder from(RandomAccessInputStream rais) throws IOException {
        SeekableInputStream sis = new DelegatingSeekableInputStream(rais) {
            @Override
	    public long getPos() throws IOException {
                return rais.getStreamOffset();
            }

            @Override
	    public void seek(long newPos) throws IOException {
                rais.seek(newPos);
            }
        };
        StreamParquetReader reader = new StreamParquetReader(new StreamInputFile(() -> sis, rais.getSize()));
        ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
        return new ParquetDecoder(reader, converter);
    }

    @Override
    public List<Record<Struct>> next(int batchSize) throws IOException {
        Double tryProgress = reader.getProgress();
        if (tryProgress == null || tryProgress >= 1) {
            log.debug("Not reading next records due to progress: " + tryProgress);
            return null;
        }
        List<Record<Struct>> result = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            SimpleGroup group = (SimpleGroup) reader.read();
            if (group == null) {
                return result;
            }
            Struct struct = converter.convert(group);
            result.add(new Record<>(struct, null, reader.getProgress(), struct.schema()));
        }
        return result;
    }

    @Override
    public void skipFirstBytes(final long numBytes) throws IOException {
        throw new CodecError("Not supported");
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
