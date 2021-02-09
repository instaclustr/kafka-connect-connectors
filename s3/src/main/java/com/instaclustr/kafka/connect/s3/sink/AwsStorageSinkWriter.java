package com.instaclustr.kafka.connect.s3.sink;


import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.instaclustr.kafka.connect.s3.AwsConnectorStringFormats;

import java.io.IOException;

import org.apache.kafka.common.TopicPartition;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;


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
    public void writeOffsetData(TopicPartition  topicPartition ,String offsetData,String filename) throws IOException, InterruptedException {
    	InputStream targetStream = new ByteArrayInputStream(offsetData.getBytes());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(offsetData.getBytes().length);
        PutObjectRequest request = new PutObjectRequest(bucketName
                , AwsConnectorStringFormats.topicPartitionOffSetStorageName(keyPrefix, topicPartition,filename)
                , targetStream
                , metadata); 
        Upload upload = transferManager.upload(request);
        upload.waitForCompletion();
    }
}