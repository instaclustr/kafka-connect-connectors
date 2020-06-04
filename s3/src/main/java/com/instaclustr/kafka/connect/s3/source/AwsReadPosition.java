package com.instaclustr.kafka.connect.s3.source;

import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.instaclustr.kafka.connect.s3.AwsConnectorStringFormats;

import java.util.Iterator;

/**
 * This class is used to keep the information required to mark a read position within a defined AWS s3 prefix
 */

public class AwsReadPosition {
    public final S3Objects s3Objects;
    private long lastReadOffset;
    private String lastReadMarker;
    private Iterator<S3ObjectSummary> currentIterator;

    public AwsReadPosition(final S3Objects s3Objects, final long lastReadOffset) {
        this.s3Objects = s3Objects;
        this.s3Objects.withDelimiter(AwsConnectorStringFormats.AWS_S3_DELIMITER);
        this.lastReadOffset = lastReadOffset;
        this.lastReadMarker = s3Objects.getMarker() == null ? s3Objects.getPrefix() : s3Objects.getMarker();
        this.currentIterator = s3Objects.iterator();
    }

    public void renewIterator(String markerToRenewWith) {
        this.lastReadMarker = markerToRenewWith;
        this.s3Objects.withMarker(markerToRenewWith);
        this.currentIterator = this.s3Objects.iterator();
    }

    public long getLastReadOffset() {
        return lastReadOffset;
    }

    public void setLastReadOffset(long lastReadOffset) {
        this.lastReadOffset = lastReadOffset;
    }

    public String getLastReadMarker() {
        return lastReadMarker;
    }

    public void setLastReadMarker(String lastReadMarker) {
        this.lastReadMarker = lastReadMarker;
    }

    public Iterator<S3ObjectSummary> getCurrentIterator() {
        return currentIterator;
    }
}
