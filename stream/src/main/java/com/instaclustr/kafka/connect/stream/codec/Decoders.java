package com.instaclustr.kafka.connect.stream.codec;

import com.instaclustr.kafka.connect.stream.Endpoint;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.ExactlyOnceSupport;

import java.io.IOException;
import java.util.Map;

public class Decoders {

    public static final String DECODER_TYPE = "decoder.type";
    public static final String PARQUET = "parquet";
    public static final String TEXT = "text";
    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(
            DECODER_TYPE,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.CaseInsensitiveValidString.in(PARQUET, TEXT),
            ConfigDef.Importance.HIGH,
            "Decoder type");

    public static Decoder<?> of(Endpoint endpoint, String filename, Map<String, String> config) throws IOException {
        String decoderType = decoderTypeOf(config);
        switch (decoderType) {
            case PARQUET:
                return ParquetDecoder.from(endpoint.openRandomAccessInputStream(filename));
            case TEXT:
                return CharDecoder.of(endpoint.openInputStream(filename), config);
            default:
                throw new UnsupportedOperationException(decoderType);
        }
    }

    public static ExactlyOnceSupport exactlyOnceSupportOf(Map<String, String> config) {
        String decoderType = decoderTypeOf(config);
        switch (decoderType) {
            case PARQUET:
                return ExactlyOnceSupport.UNSUPPORTED;
            case TEXT:
                return ExactlyOnceSupport.SUPPORTED;
            default:
                throw new UnsupportedOperationException(decoderType);
        }
    }

    private static String decoderTypeOf(Map<String, String> config) {
        AbstractConfig validConfig = new AbstractConfig(CONFIG_DEF, config);
        return validConfig.getString(DECODER_TYPE).trim().toLowerCase();
    }
}
