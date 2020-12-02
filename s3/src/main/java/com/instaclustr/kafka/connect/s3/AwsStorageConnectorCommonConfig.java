package com.instaclustr.kafka.connect.s3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Region;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Common sink/source configuration properties
 */

public class AwsStorageConnectorCommonConfig {
    public static final String BUCKET = "aws.s3.bucket";

    public static final String AWS_REGION = "aws.region";

    public static final String S3_KEY_PREFIX = "prefix"; 

    public static final String DEFAULT_AWS_REGION = Regions.DEFAULT_REGION.getName();

    private AwsStorageConnectorCommonConfig() {}

    public static ConfigDef conf() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(BUCKET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Name of the S3 bucket")
                .define(S3_KEY_PREFIX, ConfigDef.Type.STRING, "", new RegexStringValidator(Pattern.compile("^$|[-a-zA-Z0-9_./]+$"), "prefix can only contain alphanumerics, underscores(_), hyphens(-), periods(.) and slashes(/) only."),
                        ConfigDef.Importance.HIGH, "Path prefix for the objects written into S3")
                .define(AWS_REGION, ConfigDef.Type.STRING, DEFAULT_AWS_REGION, ConfigDef.Importance.MEDIUM, String.format("AWS client region, if not set will use %s", DEFAULT_AWS_REGION));
        return configDef;
    }

    private static void addErrorMessageToConfigObject(final Config config, String key, String errorMessage) {
        ConfigValue configValue = config.configValues().stream().filter(value -> key.equals(value.name())).findFirst().orElseGet(() -> {
            ConfigValue value = new ConfigValue(key);
            config.configValues().add(value);
            return value;
        });
        configValue.addErrorMessage(errorMessage);
    }

    public static void verifyS3CredentialsAndBucketInfo(final Map<String, String> sentConfigMap, final Config configObject) {
        try {
            String s3BucketName = sentConfigMap.get(BUCKET);
            String awsRegion = sentConfigMap.get(AWS_REGION);
            AmazonS3 s3Client = TransferManagerProvider.getS3ClientBuilderWithRegionAndCredentials(sentConfigMap).build();
            if (s3Client.doesBucketExistV2(s3BucketName)) {
                if (awsRegion != null) {
                    String bucketRegion = Region.fromValue(s3Client.getBucketLocation(s3BucketName)).toAWSRegion().getName();
                    if (!bucketRegion.equals(awsRegion)) {
                        addErrorMessageToConfigObject(configObject, AWS_REGION, String.format("Defined region(%s) is not the same as the bucket region(%s)", awsRegion, bucketRegion));
                    }
                }
            } else {
                addErrorMessageToConfigObject(configObject, BUCKET, "The defined bucket name does not exist");
            }
            s3Client.shutdown();
        } catch (AmazonS3Exception e) {
            switch (e.getErrorCode()) {
	            case "InvalidAccessKeyId":
	                addErrorMessageToConfigObject(configObject, "", "The AWS AccessKeyId is invalid");
	                break;
	            case "SignatureDoesNotMatch":
	                addErrorMessageToConfigObject(configObject, "", "The AWS SecretKey is invalid");
	                break;
                case "InvalidBucketName":
                    addErrorMessageToConfigObject(configObject, BUCKET, "The defined bucket name is invalid");
                    break;
                case "IllegalLocationConstraintException":
                    addErrorMessageToConfigObject(configObject, AWS_REGION, String.format("Defined region(%s) is not the same as the bucket region", sentConfigMap.get(AWS_REGION)));
                    break;
                default:
                    throw new ConnectException(String.format("Unknown Amazon S3 exception while validating config, %s", e.getErrorCode()), e);
            }
        } catch (IllegalArgumentException e) {
            addErrorMessageToConfigObject(configObject, AWS_REGION, "The defined aws.region is invalid");
        }
    }

}
