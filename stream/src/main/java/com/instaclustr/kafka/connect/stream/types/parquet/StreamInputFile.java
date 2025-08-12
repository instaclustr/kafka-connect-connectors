package com.instaclustr.kafka.connect.stream.types.parquet;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class StreamInputFile implements InputFile {

    private final Supplier<SeekableInputStream> supply;
    private final long length;

    public StreamInputFile(Supplier<SeekableInputStream> supply, long length) {
        this.supply = supply;
        this.length = length;
    }

    @Override
    public long getLength() throws IOException {
        return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        SeekableInputStream result = supply.get();
        if (result == null) {
            throw new IOException("Unable to supply seekable input stream");
        }
        return result;
    }
}
