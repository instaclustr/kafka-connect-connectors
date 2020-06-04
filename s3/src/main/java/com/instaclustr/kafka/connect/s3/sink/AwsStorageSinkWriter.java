package com.instaclustr.kafka.connect.s3.sink;


import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.instaclustr.kafka.connect.s3.AwsConnectorStringFormats;

import java.io.IOException;


public class AwsStorageSinkWriter {
    private TransferManager transferManager;
    private String bucketName;
    private String keyPrefix;

    public AwsStorageSinkWriter(final TransferManager transferManager, final String bucket, final String keyPrefix) {
        this.transferManager = transferManager;
        this.bucketName = bucket;
        this.keyPrefix = AwsConnectorStringFormats.parseS3Prefix(keyPrefix);
    }

    public void writeDataSegment(final TopicPartitionBuffer topicPartitionBuffer) throws IOException, InterruptedException {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(topicPartitionBuffer.getInputStreamLength());
        PutObjectRequest request = new PutObjectRequest(bucketName
                , AwsConnectorStringFormats.topicPartitionBufferStorageName(keyPrefix, topicPartitionBuffer)
                , topicPartitionBuffer.getInputStream()
                , metadata);
        Upload upload = transferManager.upload(request);
        upload.waitForCompletion();
        topicPartitionBuffer.cleanResources();
    }
}