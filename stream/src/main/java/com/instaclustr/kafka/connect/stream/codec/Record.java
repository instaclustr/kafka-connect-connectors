package com.instaclustr.kafka.connect.stream.codec;

import org.apache.kafka.connect.data.Schema;

public class Record<T> {

    private final T record;
    private final Long streamOffset;
    private final Double streamProgress;
    private final Schema schema;

    public Record(T record, Long streamOffset, Double streamProgress, Schema schema) {
        assert streamOffset == null || streamOffset >= 0 : "expect nonnegative stream offset if nonnull";
        assert streamProgress == null || streamProgress <= 1 : "expect nonnegative stream offset if nonnull";
        this.record = record;
        this.streamOffset = streamOffset;
        this.streamProgress = streamProgress;
        this.schema = schema;
    }

    public T getRecord() {
        return record;
    }

    /**
     * Offset for the next record position, logical to the source system.
     *
     * @return null if not using stream offset; else a nonnegative number.
     */
    public Long getStreamOffset() {
        return streamOffset;
    }

    public Double getStreamProgress() {
        return streamProgress;
    }

    public Schema getSchema() {
        return schema;
    }
}

