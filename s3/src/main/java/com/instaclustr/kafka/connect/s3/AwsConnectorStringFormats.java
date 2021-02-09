package com.instaclustr.kafka.connect.s3;

import com.instaclustr.kafka.connect.s3.sink.TopicPartitionBuffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;

import java.util.regex.Pattern;

public class AwsConnectorStringFormats {
    public static final Pattern S3_OBJECT_KEY_PATTERN = Pattern.compile("^.*?([^/]+)/([0-9]+)/([0-9]+)-([0-9]+)$");
    public static final String S3_OBJECT_KEY_FORMAT = "%s%s/%d/%s-%s"; //prefix,topic,partition,%019d start offset, %019d end offset
    public static final String S3_OBJECT_OFFSET_KEY_FORMAT = "%s%s/%d/%s/%s"; //prefix,topic,partition,folder,filename
    public static final String S3_OFFSET_FOLDER = "offset"; 
    public static final String AWS_S3_DELIMITER = "/";

    private AwsConnectorStringFormats(){}

    public static String parseS3Prefix(String value) {
        String prefix = "";
        if (StringUtils.isNotBlank(value)) {
            prefix = value.trim();
            if (!prefix.endsWith(AWS_S3_DELIMITER)) {
                prefix += AWS_S3_DELIMITER;
            }
        }
        return prefix;
    }

    public static String convertLongIntoLexySortableString(long value){
        return String.format("%019d", value);
    }

    public static String topicPartitionBufferStorageName(String keyPrefix, TopicPartitionBuffer topicPartitionBuffer) {
        return String.format(AwsConnectorStringFormats.S3_OBJECT_KEY_FORMAT,
                keyPrefix,
                topicPartitionBuffer.topic,
                topicPartitionBuffer.partition,
                AwsConnectorStringFormats.convertLongIntoLexySortableString(topicPartitionBuffer.getStartOffset()),
                AwsConnectorStringFormats.convertLongIntoLexySortableString(topicPartitionBuffer.getEndOffset()));
    }
    public static String topicPartitionOffSetStorageName(String keyPrefix, TopicPartition topicPartition,String filename) {
        return String.format(AwsConnectorStringFormats.S3_OBJECT_OFFSET_KEY_FORMAT,
                keyPrefix,
                topicPartition.topic(),
                topicPartition.partition(),
                S3_OFFSET_FOLDER,
                filename);
    }
    public static String generateTargetTopic(String topicPrefix, String topic){
        return String.format("%s%s", (StringUtils.isBlank(topicPrefix) ? "" : topicPrefix + "-"), topic);
    }
}
