package com.instaclustr.kafka.connect.stream.endpoint;

import com.instaclustr.kafka.connect.stream.ExtentInputStream;
import org.apache.kafka.common.config.ConfigDef;

import java.io.IOException;
import java.io.InputStream;

public interface ExtentBased {
    String EXTENT_STRIDE = "extent.stride";

    ConfigDef CONFIG_DEF = new ConfigDef()
            .define(EXTENT_STRIDE,
                    ConfigDef.Type.LONG,
                    ExtentInputStream.DEFAULT_EXTENT_STRIDE,
                    ConfigDef.Range.atLeast(8),
                    ConfigDef.Importance.LOW,
                    "Stream byte range per open request");

    InputStream openInputStream(String filename, long extentStart, long extentStride) throws IOException;
}
