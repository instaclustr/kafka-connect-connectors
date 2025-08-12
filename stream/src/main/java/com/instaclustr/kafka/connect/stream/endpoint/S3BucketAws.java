package com.instaclustr.kafka.connect.stream.endpoint;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.instaclustr.kafka.connect.stream.Endpoint;
import org.apache.kafka.common.config.AbstractConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class S3BucketAws extends S3Bucket implements AccessKeyBased {

    // Visible for testing
    S3BucketAws(TransferManager transferManager, String bucketName, long extentStride) {
        super(transferManager, bucketName, extentStride);
    }

    public static S3BucketAws of(Map<String, String> providedConf) {
        AbstractConfig s3BucketConf = new AbstractConfig(S3Bucket.CONFIG_DEF, providedConf);
        AbstractConfig accessKeyConf = new AbstractConfig(AccessKeyBased.CONFIG_DEF, providedConf);
        AbstractConfig extentConf = new AbstractConfig(ExtentBased.CONFIG_DEF, providedConf);

        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(accessKeyConf.getString(ACCESS_KEY_ID), accessKeyConf.getPassword(ACCESS_KEY).value())
        );

        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(awsStaticCredentialsProvider)
                .withRegion(Regions.fromName(s3BucketConf.getString(REGION)).getName())
                .withClientConfiguration(
                        new ClientConfiguration()
                                .withMaxErrorRetry(5)
                                .withTcpKeepAlive(true)
                );

        TransferManagerBuilder transferBuilder = TransferManagerBuilder.standard()
                .withS3Client(
                        clientBuilder.build()
                );

        return new S3BucketAws(
                transferBuilder.build(), s3BucketConf.getString(BUCKET_NAME), extentConf.getLong(EXTENT_STRIDE));
    }

}
