package com.instaclustr.kafka.connect.stream.endpoint;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.instaclustr.kafka.connect.stream.ExtentInputStream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class S3BucketTest {

    @Test
    public void testList() throws IOException {
        AmazonS3 s3Client = Mockito.mock(AmazonS3.class);
        TransferManager tm = mock(TransferManager.class);
        when(tm.getAmazonS3Client()).thenReturn(s3Client);
        S3Bucket s3Bucket = new S3BucketAws(tm, "testBucket", ExtentInputStream.DEFAULT_EXTENT_STRIDE);

        List<ObjectListing> pages = Stream.generate(() -> mock(ObjectListing.class)).limit(3).collect(Collectors.toList());
        for (int i = 0; i < pages.size(); i++) {
            S3ObjectSummary si = new S3ObjectSummary();
            si.setKey(String.valueOf(i));
            when(pages.get(i).getObjectSummaries()).thenReturn(List.of(si));
            if (i == 0) {
                // First page
                when(s3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(pages.get(i));
            } else {
                // Non-first pages
                when(s3Client.listNextBatchOfObjects(pages.get(i - 1))).thenReturn(pages.get(i));
            }
            when(pages.get(i).isTruncated()).thenReturn(i < pages.size() - 1);
        }

        for (int i = 0; i < pages.size(); i++) {
            S3ObjectSummary si = new S3ObjectSummary();
            si.setKey(String.valueOf(i));
            when(pages.get(i).getObjectSummaries()).thenReturn(List.of(si));
        }

        when(s3Client.listObjects(any(), any())).thenReturn(pages.get(0));
        for (int i = 1; i < pages.size(); i++) {
            when(s3Client.listNextBatchOfObjects(pages.get(i - 1))).thenReturn(pages.get(i));
        }

        List<String> result = s3Bucket.listRegularFiles("testPath").collect(Collectors.toList());

        Assert.assertEquals(result, List.of("0","1","2"));
    }
}
