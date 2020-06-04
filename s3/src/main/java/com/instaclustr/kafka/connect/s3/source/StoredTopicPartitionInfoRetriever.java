package com.instaclustr.kafka.connect.s3.source;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.instaclustr.kafka.connect.s3.AwsConnectorStringFormats;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class handles exploring the defined s3 prefix for topics and partitions to be processed by AwsStorageSourceConnector
 */

public class StoredTopicPartitionInfoRetriever {
    private static Logger log = LoggerFactory.getLogger(StoredTopicPartitionInfoRetriever.class);
    private List<String> userDefinedTopics;
    private String awsS3Bucket;
    private String awsS3Prefix;
    private TransferManager transferManager;

    public StoredTopicPartitionInfoRetriever(final String awsS3Bucket, final String awsS3Prefix, final String userDefinedTopicsString, final TransferManager transferManager) {
        if (StringUtils.isEmpty(userDefinedTopicsString)) {
            userDefinedTopics = Collections.emptyList();
        } else {
            userDefinedTopics = Arrays.asList(userDefinedTopicsString.split(","));
        }
        this.awsS3Bucket = awsS3Bucket;
        this.awsS3Prefix = AwsConnectorStringFormats.parseS3Prefix(awsS3Prefix);
        this.transferManager = transferManager;
    }

    public List<String> getStoredTopicsAndPartitionInfo() {
        List<String> topicPartitions = new ArrayList<>();
        List<String> topicListings = this.userDefinedTopics.isEmpty() ?
                getCommonPrefixesForAPrefixFromS3(this.awsS3Prefix) :
                this.userDefinedTopics.stream().map(topic -> String.format("%s%s%s", this.awsS3Prefix, topic, AwsConnectorStringFormats.AWS_S3_DELIMITER)).collect(Collectors.toList());
        for (String topicListing : topicListings) {
            String topicString = topicListing.replaceFirst(this.awsS3Prefix, "").replace(AwsConnectorStringFormats.AWS_S3_DELIMITER, "");
            log.debug("Found topic {}", topicString);
            if (this.userDefinedTopics.isEmpty() || this.userDefinedTopics.contains(topicString)) {
                log.debug("Looking for partitions under {}", topicListing);
                List<String> partitionListings = getCommonPrefixesForAPrefixFromS3(topicListing);
                for (String partitionListing : partitionListings) {
                    log.debug("Found partition {}", partitionListing);
                    String partitionString = partitionListing.replaceFirst(topicListing, "").replace(AwsConnectorStringFormats.AWS_S3_DELIMITER, "");
                    try {
                        int partition = Integer.parseInt(partitionString);
                        topicPartitions.add(String.format("%s%s%s", topicString, AwsConnectorStringFormats.AWS_S3_DELIMITER, partition));
                    } catch (NumberFormatException e) {
                        log.warn("Found non parsable prefix entry within topic {}", topicString);
                    }
                }
            }
        }
        topicPartitions.sort(Comparator.naturalOrder());
        return topicPartitions;
    }

    public List<String> getCommonPrefixesForAPrefixFromS3(String prefix) {
        Set<String> commonPrefixes = new TreeSet<>();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setDelimiter(AwsConnectorStringFormats.AWS_S3_DELIMITER);
        listObjectsRequest.setBucketName(this.awsS3Bucket);
        listObjectsRequest.setPrefix(prefix);
        ObjectListing objectListing;
        do {
            objectListing = this.transferManager.getAmazonS3Client().listObjects(listObjectsRequest);
            commonPrefixes.addAll(objectListing.getCommonPrefixes());
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());
        return new ArrayList<>(commonPrefixes);
    }
}
