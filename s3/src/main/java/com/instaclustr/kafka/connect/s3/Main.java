package com.instaclustr.kafka.connect.s3;

import com.instaclustr.kafka.connect.s3.source.AwsStorageSourceConnector;
import org.apache.kafka.common.config.Config;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(AwsStorageConnectorCommonConfig.BUCKET, "niluka-custom-connector");
        configMap.put(AwsStorageConnectorCommonConfig.AWS_REGION, "us-east-1");
        configMap.put(AwsStorageConnectorCommonConfig.S3_KEY_PREFIX, "niluka/");
        configMap.put(AwsStorageConnectorCommonConfig.AWS_IAM_ROLE_ARN, "");

        AwsStorageSourceConnector connector = new AwsStorageSourceConnector();
        Config configObject = connector.validate(configMap);
        connector.checkObjects(configMap);

        // Print the config object to see the validation results
        configObject.configValues().forEach(configValue -> {
            System.out.println("Name: " + configValue.name());
            System.out.println("Value: " + configValue.value());
            System.out.println("Errors: " + configValue.errorMessages());
        });
    }
}