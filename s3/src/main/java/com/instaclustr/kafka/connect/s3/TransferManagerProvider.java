package com.instaclustr.kafka.connect.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
        String accessKey = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_ACCESS_KEY_ID);
        String secret = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_SECRET_KEY);
        String region = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_REGION);
        String roleArn = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_IAM_ROLE_ARN);

        AmazonS3ClientBuilder clientBuilder;

        if (roleArn == null || StringUtils.isBlank(roleArn)) {
            // authenticate with access key/secret
            AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secret));
            clientBuilder = AmazonS3ClientBuilder.standard()
                    .withCredentials(awsStaticCredentialsProvider);
        } else {
            // authenticate with access key/secret, then assume role
            AWSSecurityTokenService awsSecurityTokenService = AWSSecurityTokenServiceClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secret)))
                    .build();

            STSAssumeRoleSessionCredentialsProvider.Builder assumeRoleBuilder =
                    new STSAssumeRoleSessionCredentialsProvider.Builder(
                            roleArn,
                            UUID.randomUUID().toString().substring(0, 32));

            STSAssumeRoleSessionCredentialsProvider credentialsProvider = assumeRoleBuilder
                    .withStsClient(awsSecurityTokenService)
                    .withRoleSessionDurationSeconds((int) TimeUnit.HOURS.toSeconds(1))
                    .build();

            clientBuilder = AmazonS3ClientBuilder.standard()
                    .withCredentials(credentialsProvider);
        }

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
