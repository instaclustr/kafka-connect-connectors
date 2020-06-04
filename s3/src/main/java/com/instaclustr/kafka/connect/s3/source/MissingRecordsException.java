package com.instaclustr.kafka.connect.s3.source;

public class MissingRecordsException extends RuntimeException {
    public MissingRecordsException(final String message) {
        super(message);
    }
}
