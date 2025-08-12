package com.instaclustr.kafka.connect.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

public interface Endpoint {

    InputStream openInputStream(String path) throws IOException;

    RandomAccessInputStream openRandomAccessInputStream(String path) throws IOException;

    Stream<String> listRegularFiles(String path) throws IOException;

    long getFileSize(String path) throws IOException;
}
