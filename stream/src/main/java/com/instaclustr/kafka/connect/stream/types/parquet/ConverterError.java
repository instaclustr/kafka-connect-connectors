package com.instaclustr.kafka.connect.stream.types.parquet;

public class ConverterError extends RuntimeException {

    public ConverterError(String message) {
        super(message);
    }
}
