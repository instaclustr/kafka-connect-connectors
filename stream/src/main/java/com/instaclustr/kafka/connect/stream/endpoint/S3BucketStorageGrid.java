package com.instaclustr.kafka.connect.stream.endpoint;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.Map;

public class S3BucketStorageGrid extends S3Bucket implements AccessKeyBased {

    private S3BucketStorageGrid(TransferManager transferManager, String bucketName, long extentStride) {
        super(transferManager, bucketName, extentStride);
    }

    public static S3BucketStorageGrid of(Map<String, String> providedConf) {
        AbstractConfig s3BucketConf = new AbstractConfig(S3Bucket.CONFIG_DEF, providedConf);
        AbstractConfig accessKeyConf = new AbstractConfig(AccessKeyBased.CONFIG_DEF, providedConf);
        AbstractConfig extentConf = new AbstractConfig(ExtentBased.CONFIG_DEF, providedConf);

        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(accessKeyConf.getString(ACCESS_KEY_ID), accessKeyConf.getPassword(ACCESS_KEY).value())
        );

        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                s3BucketConf.getString(S3Bucket.URL),
                s3BucketConf.getString(S3Bucket.REGION)
        );

        // Path-style URL access is deprecated:
        // https://docs.netapp.com/us-en/storagegrid/ilm/creating-cloud-storage-pool.html
        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(awsStaticCredentialsProvider)
                .withClientConfiguration(
                        new ClientConfiguration()
                                .withMaxErrorRetry(5)
                                .withTcpKeepAlive(true))
                .withPathStyleAccessEnabled(false)
                .withEndpointConfiguration(endpointConfiguration);

        TransferManagerBuilder transferBuilder = TransferManagerBuilder.standard()
                .withS3Client(
                        clientBuilder.build()
                );

        return new S3BucketStorageGrid(
                transferBuilder.build(), s3BucketConf.getString(BUCKET_NAME), extentConf.getLong(EXTENT_STRIDE));
    }
}
