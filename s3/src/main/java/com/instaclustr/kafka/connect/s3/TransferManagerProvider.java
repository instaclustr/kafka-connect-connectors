package com.instaclustr.kafka.connect.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransferManagerProvider {
    private TransferManager transferManager;
    private static Logger log = LoggerFactory.getLogger(TransferManagerProvider.class);

    public TransferManagerProvider(final Map<String, String> config) {

        AmazonS3ClientBuilder clientBuilder = getS3ClientBuilderWithRegionAndCredentials(config)
                .withClientConfiguration(new ClientConfiguration()
                        .withMaxErrorRetry(5)
                        .withTcpKeepAlive(true)
                );
        transferManager = TransferManagerBuilder.standard().withS3Client(clientBuilder.build()).build();
    }

    public static AmazonS3ClientBuilder getS3ClientBuilderWithRegionAndCredentials(final Map<String, String> config) {
        String region = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_REGION);

        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain());

        if (region == null) {
            region = AwsStorageConnectorCommonConfig.DEFAULT_AWS_REGION;
            clientBuilder.enableForceGlobalBucketAccess();
            log.info("No region defined. Using {} and force global bucket access", AwsStorageConnectorCommonConfig.DEFAULT_AWS_REGION);
        }
        clientBuilder.withRegion(Regions.fromName(region).getName()); //using fromName to validate the region value
        return clientBuilder;
    }

    private static String getFromConfigOrEnvironment(final Map<String, String> config, final String key) {
        String retVal = System.getProperty(key);
        if (config.containsKey(key)) {
            retVal = config.get(key);
        }
        return retVal;
    }

    public TransferManager get() {
        return transferManager;
    }
}
