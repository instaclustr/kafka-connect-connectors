package com.instaclustr.kafka.connect.s3.source;

public class EndOfSourceStreamException extends Exception {
    public EndOfSourceStreamException() {
        super();
    }

    public EndOfSourceStreamException(final String message) {
        super(message);
    }
}
