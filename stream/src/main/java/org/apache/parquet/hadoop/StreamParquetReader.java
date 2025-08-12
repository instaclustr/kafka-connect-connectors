package org.apache.parquet.hadoop; // in this package due to referencing package-local InternalParquetRecordReader class

import com.instaclustr.kafka.connect.stream.types.parquet.StreamInputFile;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.ParquetReadOptions;

public class StreamParquetReader implements Closeable {

    private final GroupReadSupport readSupport;
    private final Iterator<StreamInputFile> files;
    private final ParquetReadOptions options;

    private InternalParquetRecordReader<Group> reader;

    public StreamParquetReader(StreamInputFile file) throws IOException {
        readSupport = new GroupReadSupport();

        files = Collections.singletonList(file).iterator();

        ParquetReadOptions.Builder optionsBuilder = ParquetReadOptions.builder();
        ByteBufferAllocator allocator = new HeapByteBufferAllocator();
        options = optionsBuilder.withAllocator(allocator).build();

        reader = null;
    }

    public Group read() throws IOException {
        try {
            if (reader != null && reader.nextKeyValue()) {
                return reader.getCurrentValue();
            } else {
                initReader();
                return reader == null ? null : read();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public Double getProgress() throws IOException {
        try {
            if (reader != null) {
                return (double) reader.getProgress();
            } else {
                initReader();
                return reader == null ? null : (double) reader.getProgress();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    private void initReader() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (files.hasNext()) {
            StreamInputFile file = files.next();
            ParquetFileReader fileReader = new ParquetFileReader(file, options, file.newStream());
            reader = new InternalParquetRecordReader<>(readSupport, options.getRecordFilter());
            reader.initialize(fileReader, options);
        }
    }
}
