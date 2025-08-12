package com.instaclustr.kafka.connect.stream.codec;

import org.testng.annotations.Test;

import java.util.HashMap;

import static com.instaclustr.kafka.connect.stream.codec.Decoders.*;
import static org.apache.kafka.connect.source.ExactlyOnceSupport.SUPPORTED;
import static org.apache.kafka.connect.source.ExactlyOnceSupport.UNSUPPORTED;
import static org.testng.Assert.assertEquals;

public class DecodersTest {
    @Test
    public void exactlyOnceSupport() {
        var config = new HashMap<String, String>();
        config.put(DECODER_TYPE, TEXT);
        assertEquals(exactlyOnceSupportOf(config), SUPPORTED);

        config.put(DECODER_TYPE, PARQUET);
        assertEquals(exactlyOnceSupportOf(config), UNSUPPORTED);
    }
}
