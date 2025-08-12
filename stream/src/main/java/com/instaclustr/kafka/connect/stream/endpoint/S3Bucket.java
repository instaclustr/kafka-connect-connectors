package com.instaclustr.kafka.connect.stream.endpoint;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.instaclustr.kafka.connect.stream.Endpoint;
import com.instaclustr.kafka.connect.stream.ExtentInputStream;
import com.instaclustr.kafka.connect.stream.RandomAccessInputStream;
import org.apache.kafka.common.config.ConfigDef;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;

import static com.instaclustr.kafka.connect.stream.Util.doWhile;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public abstract class S3Bucket implements Endpoint, ExtentBased {
    public static String BUCKET_NAME = "s3.bucket.name";
    public static String REGION = "s3.region";
    public static String URL = "s3.url";

    public static ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BUCKET_NAME, STRING, null, new ConfigDef.NonEmptyStringWithoutControlChars(), HIGH, "S3 bucket name")
            .define(REGION, STRING, null, new ConfigDef.NonEmptyStringWithoutControlChars(),  HIGH, "S3 Region")
            .define(URL, STRING, null, new ConfigDef.NonEmptyStringWithoutControlChars(), HIGH, "S3 URL");

    private final TransferManager transferManager;
    private final String bucketName;
    private final long extentStride;

    public S3Bucket(TransferManager transferManager, String bucketName, long extentStride) {
        this.transferManager = transferManager;
        this.bucketName = bucketName;
        this.extentStride = extentStride;
    }

    @Override
    public InputStream openInputStream(String objectKey) throws IOException {
        return openRandomAccessInputStream(objectKey);
    }

    @Override
    public InputStream openInputStream(String objectKey, long extentStart, long extentStride) throws IOException {
        try {
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, objectKey)
                    .withRange(extentStart, extentStart + extentStride - 1);
            return Objects.requireNonNull(
                    getClient().getObject(getObjectRequest).getObjectContent(),
                    "No stream found");
        } catch (SdkClientException e) {
            throw new IOException (e);
        }
    }

    @Override
    public RandomAccessInputStream openRandomAccessInputStream(String objectKey) throws IOException {
        try {
            return ExtentInputStream.of(objectKey, getFileSize(objectKey), this, extentStride);
        } catch (SdkClientException e) {
            throw new IOException (e);
        }
    }

    @Override
    public Stream<String> listRegularFiles(String path) throws IOException {
        try {
            ListObjectsRequest request = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(path)
                    .withDelimiter(null); // null delimiter for recursive nested listing
            ObjectListing seed = getClient().listObjects(request);
            Stream<ObjectListing> ls = doWhile(seed, ObjectListing::isTruncated, l -> getClient().listNextBatchOfObjects(l));
            return ls.flatMap(l -> l.getObjectSummaries().stream().map(S3ObjectSummary::getKey)).filter(S3Bucket::isRegularFile);
        } catch (SdkClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long getFileSize(String path) throws IOException {
        try {
            ObjectMetadata metadata = getClient().getObjectMetadata(bucketName, path);
            return metadata.getContentLength();
        } catch (SdkClientException e) {
            throw new IOException (e);
        }
    }

    AmazonS3 getClient() {
        return transferManager.getAmazonS3Client();
    }

    static boolean isRegularFile(String path) {
        return !path.endsWith("/");
    }

}
