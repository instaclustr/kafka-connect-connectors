package com.instaclustr.kafka.connect.stream;

import java.io.InputStream;
import java.io.IOException;
import java.io.FilterInputStream;

public abstract class RandomAccessInputStream extends FilterInputStream {

    protected RandomAccessInputStream(InputStream in) {
        super(in);
    }

    public abstract void seek(long offset) throws IOException;

    public abstract long getStreamOffset() throws IOException;

    public abstract long getSize();
}
