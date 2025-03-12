package com.instaclustr.kafka.connect.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
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
        final String accessKey = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_ACCESS_KEY_ID);
        final String secret = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_SECRET_KEY);
        String region = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_REGION);
        final String roleArn = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.AWS_IAM_ROLE_ARN);
        final String endpoint = getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.S3_ENDPOINT);

        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secret));
        AWSCredentialsProvider awsCredentialsProvider;

        if (StringUtils.isBlank(roleArn)) {
            // when IAM user has direct access to the S3 bucket
            awsCredentialsProvider = awsStaticCredentialsProvider;
        } else {
            // when the IAM user needs to assume the role to access the S3 bucket
            AWSSecurityTokenService awsSecurityTokenService = AWSSecurityTokenServiceClientBuilder.standard()
                    .withCredentials(awsStaticCredentialsProvider)
                    .build();

            STSAssumeRoleSessionCredentialsProvider.Builder assumeRoleBuilder =
                    new STSAssumeRoleSessionCredentialsProvider.Builder(
                            roleArn,
                            UUID.randomUUID().toString().substring(0, 32));

            awsCredentialsProvider = assumeRoleBuilder
                    .withStsClient(awsSecurityTokenService)
                    .build();
        }

        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(awsCredentialsProvider);

        if (region == null && StringUtils.isBlank(endpoint)) {
            region = AwsStorageConnectorCommonConfig.DEFAULT_AWS_REGION;
            clientBuilder.enableForceGlobalBucketAccess();
            log.info("No region defined. Using {} and force global bucket access", AwsStorageConnectorCommonConfig.DEFAULT_AWS_REGION);
        }

        if (StringUtils.isNotBlank(endpoint)) {
            final boolean isPathStyleAccessEnabled = Boolean.parseBoolean(getFromConfigOrEnvironment(config, AwsStorageConnectorCommonConfig.S3_ENABLE_PATH_STYLE));

            AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(endpoint, region);
            clientBuilder.withEndpointConfiguration(endpointConfiguration).withPathStyleAccessEnabled(isPathStyleAccessEnabled);
        } else {
            clientBuilder.withRegion(Regions.fromName(region).getName()); //using fromName to validate the region value
        }

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
