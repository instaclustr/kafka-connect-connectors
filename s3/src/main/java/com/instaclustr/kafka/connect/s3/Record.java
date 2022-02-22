package com.instaclustr.kafka.connect.s3;

public class Record {
    private final String k;
    private final String v;
    private final Long t;
    private final long o;

    Record(String k, String v, Long t, long o){
        this.k = k;
        this.v = v;
        this.t = t;
        this.o = o;
    }
}
