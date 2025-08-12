package com.instaclustr.kafka.connect.stream.codec;

import java.io.IOException;

public class CodecError extends IOException {

    public CodecError(String msg) {
        super(msg);
    }

    public CodecError(Exception e) {
        super(e);
    }
}
