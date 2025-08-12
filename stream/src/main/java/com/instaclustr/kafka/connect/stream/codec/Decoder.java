package com.instaclustr.kafka.connect.stream.codec;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Decoder<T> extends Closeable {
    
    /**
     * Get next batch of decoded records.
     * @param batchSize
     * @return null if underlying stream has no bytes available to read yet; an empty list if underlying stream has
     * some available bytes to read but not enough yet for decoding a single record; otherwise a nonempty list of size
     * no more than the batchSize.
     * @throws IOException
     */
    List<Record<T>> next(int batchSize) throws IOException;

    void skipFirstBytes(final long numBytes) throws IOException;
}
