package com.instaclustr.kafka.connect.stream.endpoint;

import com.instaclustr.kafka.connect.stream.Endpoint;
import com.instaclustr.kafka.connect.stream.ExtentInputStream;
import com.instaclustr.kafka.connect.stream.RandomAccessInputStream;
import org.apache.kafka.common.config.AbstractConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

public class LocalFile implements Endpoint, ExtentBased {
    public static LocalFile of(Map<String, String> props) {
        AbstractConfig extentConf = new AbstractConfig(ExtentBased.CONFIG_DEF, props);
        return new LocalFile(extentConf.getLong(ExtentBased.EXTENT_STRIDE));
    }
    private final long extentStride;

    public LocalFile(long extentStride) {
        this.extentStride = extentStride;
    }

    @Override
    public InputStream openInputStream(final String streamName) throws IOException {
        return Files.newInputStream(Path.of(streamName));
    }

    @Override
    public InputStream openInputStream(String filename, long extentStart, long extentStride) throws IOException {
        InputStream is = openInputStream(filename);
        long remaining = extentStart;
        while (remaining > 0) {
            remaining -= is.skip(extentStart);
        }
        return is;
    }

    @Override
    public RandomAccessInputStream openRandomAccessInputStream(final String streamName) throws IOException {
        File f = new File(streamName);
        return ExtentInputStream.of(streamName, f.length(), this, extentStride);
    }

    @Override
    public Stream<String> listRegularFiles(final String path) throws IOException {
        File f = new File(path);
        Stream<String> result;
        if (f.isDirectory()) {
            var fs = f.listFiles(File::isFile);
            if (fs == null) {
                result = Stream.empty();
            } else {
                result = Arrays.stream(fs).map(File::getAbsolutePath);
            }
        } else if (f.isFile()) {
            result = Stream.of(f.getAbsolutePath());
        } else {
            throw new IOException("Path is neither a directory nor a file: " + path);
        }
        return result;
    }

	@Override
	public long getFileSize(String path) throws IOException {
		return new File(path).length();
	}
}
