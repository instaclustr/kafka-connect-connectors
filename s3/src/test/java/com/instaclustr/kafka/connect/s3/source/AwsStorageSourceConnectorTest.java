package com.instaclustr.kafka.connect.s3.source;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.instaclustr.kafka.connect.s3.AwsStorageConnectorCommonConfig;
import com.instaclustr.kafka.connect.s3.TransferManagerProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertTrue;

public class AwsStorageSourceConnectorTest {

    private AwsStorageSourceConnector connector;
    private Map<String, String> configMap;

    @BeforeMethod
    public void setUp() {
        connector = new AwsStorageSourceConnector();
        configMap = new HashMap<>();
        configMap.put(AwsStorageConnectorCommonConfig.BUCKET, "test-bucket");
        configMap.put(AwsStorageConnectorCommonConfig.AWS_REGION, "us-west-2");
        configMap.put(AwsStorageConnectorCommonConfig.AWS_ACCESS_KEY_ID, "test-access-key-id");
        configMap.put(AwsStorageConnectorCommonConfig.AWS_SECRET_KEY, "test-secret-key");
    }

    @Test
    public void testValidateInvalidRegionConfigForAwsS3() {
        configMap.put(AwsStorageConnectorCommonConfig.AWS_REGION, "invalid-region");
        final Config config = connector.validate(configMap);
        final String regionErrorMessage = config.configValues().stream()
                .filter(configValue -> AwsStorageConnectorCommonConfig.AWS_REGION.equals(configValue.name()))
                .findFirst()
                .get()
                .errorMessages()
                .get(0);
        assertTrue(StringUtils.equals(regionErrorMessage, "The defined aws.region is invalid Cannot create enum from invalid-region value!"));
    }


    @Test
    public void testValidConfigForAwsS3() {
        configMap.put(AwsStorageConnectorCommonConfig.AWS_REGION, "us-east-1");

        AmazonS3 mockS3Client = mock(AmazonS3.class);
        AmazonS3ClientBuilder mockBuilder = mock(AmazonS3ClientBuilder.class);

        when(mockBuilder.build()).thenReturn(mockS3Client);

        try (MockedStatic<TransferManagerProvider> transferManagerMock = mockStatic(TransferManagerProvider.class)) {
            transferManagerMock.when(() -> TransferManagerProvider.getS3ClientBuilderWithRegionAndCredentials(any()))
                    .thenReturn(mockBuilder);

            when(mockS3Client.doesBucketExistV2(anyString())).thenReturn(true);

            Config returnedConfig = connector.validate(configMap);
            Assert.assertTrue(true);
            for (final ConfigValue cv : returnedConfig.configValues()) {
                assertTrue(cv.errorMessages().isEmpty());
            }
        }
    }


    @Test
    public void testValidEndpointForStoragegrid() {
        configMap.put(AwsStorageConnectorCommonConfig.AWS_REGION, "any-non-aws-region");
        configMap.put(AwsStorageConnectorCommonConfig.S3_ENDPOINT, "TestSite.com");

        AmazonS3 mockS3Client = mock(AmazonS3.class);
        AmazonS3ClientBuilder mockBuilder = mock(AmazonS3ClientBuilder.class);

        when(mockBuilder.build()).thenReturn(mockS3Client);

        try (MockedStatic<TransferManagerProvider> transferManagerMock = mockStatic(TransferManagerProvider.class)) {
            transferManagerMock.when(() -> TransferManagerProvider.getS3ClientBuilderWithRegionAndCredentials(any()))
                    .thenReturn(mockBuilder);

            when(mockS3Client.doesBucketExistV2(anyString())).thenReturn(true);

            Config returnedConfig = connector.validate(configMap);
            Assert.assertTrue(true);
            for (final ConfigValue cv : returnedConfig.configValues()) {
                assertTrue(cv.errorMessages().isEmpty());
            }
        }
    }

    @Test
    public void testValidEndpointForOntapS3() {
        configMap.put(AwsStorageConnectorCommonConfig.AWS_REGION, "any-non-aws-region");
        configMap.put(AwsStorageConnectorCommonConfig.S3_ENDPOINT, "TestSite.com");
        configMap.put(AwsStorageConnectorCommonConfig.S3_ENABLE_PATH_STYLE, "true");

        AmazonS3 mockS3Client = mock(AmazonS3.class);
        AmazonS3ClientBuilder mockBuilder = mock(AmazonS3ClientBuilder.class);

        when(mockBuilder.build()).thenReturn(mockS3Client);

        try (MockedStatic<TransferManagerProvider> transferManagerMock = mockStatic(TransferManagerProvider.class)) {
            transferManagerMock.when(() -> TransferManagerProvider.getS3ClientBuilderWithRegionAndCredentials(any()))
                    .thenReturn(mockBuilder);

            when(mockS3Client.doesBucketExistV2(anyString())).thenReturn(true);

            Config returnedConfig = connector.validate(configMap);
            Assert.assertTrue(true);
            for (final ConfigValue cv : returnedConfig.configValues()) {
                assertTrue(cv.errorMessages().isEmpty());
            }
        }
    }
}