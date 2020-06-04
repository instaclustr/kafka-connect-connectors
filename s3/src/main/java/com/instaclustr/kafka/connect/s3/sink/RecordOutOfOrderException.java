package com.instaclustr.kafka.connect.s3.sink;

public class RecordOutOfOrderException extends Exception {
    public RecordOutOfOrderException() {
        super();
    }

    public RecordOutOfOrderException(final String message) {
        super(message);
    }
}
