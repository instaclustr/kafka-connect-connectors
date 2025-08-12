package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.endpoint.ExtentBased;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ExtentInputStreamTest {

    private ExtentBased endpoint;
    private ExtentInputStream stream;
    private File tempFile; // for one extent in the stream
    private static final String FILE_NAME = "dummy-file-name"; // for entire stream

    @BeforeMethod
    public void setup() throws IOException {
        tempFile = File.createTempFile("file-stream-source-task-test", null);
        endpoint = mock(ExtentBased.class);
    }

    @AfterMethod
    public void teardown() throws IOException {
        try {
            if (stream != null) {
                stream.close();
            }
        } finally {
            Files.deleteIfExists(tempFile.toPath());
        }
    }

    @Test
    public void readEmptyFile() throws IOException {
        try (var extent = Files.newInputStream(tempFile.toPath())) {
            when(endpoint.openInputStream(anyString(), anyLong(), anyLong())).thenReturn(extent);

            long fileSize = 0;

            stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, ExtentInputStream.DEFAULT_EXTENT_STRIDE);

            assertEquals(stream.getFileOffset(), 0);
            assertEof();
            assertEquals(stream.getFileOffset(), 0);
        }
    }

    @Test
    public void readOneExtent() throws IOException {
        try (var extent = Files.newInputStream(tempFile.toPath())) {
            when(endpoint.openInputStream(anyString(), anyLong(), anyLong())).thenReturn(extent);

            var contentBytes = writeContent(tempFile, "123");

            long fileSize = contentBytes.length;
            long extentStride = fileSize + 3; // Larger than file size

            stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

            assertEquals(fileSize, 3);
            int result;
            for (int i = 0; i < fileSize; i++) {
                result = readOneByte();
                assertEquals(result, contentBytes[i]);
                assertEquals(stream.getFileOffset(), i + 1);
            }
            assertEof();
        }
    }

    private void assertEof() throws IOException {
        byte[] b = new byte[1];
        for (int i = 0; i < 10; i ++) {
            assertEquals(stream.read(b), -1);
        }
    }

    private byte[] writeContent(File tempFile, String content) throws IOException {
        final var out = Files.newOutputStream(tempFile.toPath());
        final var contentBytes = content.getBytes(StandardCharsets.UTF_8);
        out.write(contentBytes);
        out.flush();
        return contentBytes;
    }

    @Test
    public void readMultipleExtents() throws IOException {
        File tempFile2 = File.createTempFile("ExtentInputStreamTest-tempFile2", null);
        try (var extent = Files.newInputStream(tempFile.toPath());
             var extent2 = Files.newInputStream(tempFile2.toPath())) {
            when(endpoint.openInputStream(anyString(), anyLong(), anyLong()))
                    .thenReturn(extent)
                    .thenReturn(extent2);
            var contentBytes = writeContent(tempFile, "123");
            var contentBytes2 = writeContent(tempFile2, "456");
            long fileSize = contentBytes.length + contentBytes2.length;
            long extentStride = 3;
            assertEquals(contentBytes.length, extentStride);

            stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

            // first extent: 123
            int result;
            for (int i = 0; i < extentStride; i++) {
                result = readOneByte();
                assertEquals(result, contentBytes[i]);
                assertEquals(stream.getFileOffset(), i + 1);
            }

            // second extent: 456
            for (int i = 0; i < extentStride; i++) {
                result = readOneByte();
                assertEquals(result, contentBytes2[i]);
                assertEquals(stream.getFileOffset(), extentStride + i + 1);
            }

            assertEof();
        } finally {
            Files.deleteIfExists(tempFile2.toPath());
        }
    }

    private int readOneByte() throws IOException {
        byte[] b = new byte[10];
        int n = stream.read(b, 0, 1);
        assertEquals(n, 1);
        return b[0];
    }

    @Test
    public void skipFirstExtent() throws IOException {
        File tempFile2 = File.createTempFile("ExtentInputStreamTest-tempFile2", null);
        try (var extent = Files.newInputStream(tempFile.toPath());
             var extent2 = Files.newInputStream(tempFile2.toPath())) {
            when(endpoint.openInputStream(anyString(), anyLong(), anyLong()))
                    .thenReturn(extent2); // will skip extent: 123
            var contentBytes = writeContent(tempFile, "123");
            var contentBytes2 = writeContent(tempFile2, "456");
            long fileSize = contentBytes.length + contentBytes2.length;
            long extentStride = 3;
            assertEquals(contentBytes.length, extentStride);

            stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

            // skip extent: 123
            long skipped = stream.skip(extentStride);
            assertEquals(skipped, extentStride);
            assertEquals(stream.getExtentStartOffset(), extentStride);

            // second extent: 456
            int result;
            for (int i = 0; i < extentStride; i++) {
                result = readOneByte();
                assertEquals(result, contentBytes2[i]);
                assertEquals(stream.getFileOffset(), extentStride + i + 1);
            }

            assertEof();
        } finally {
            Files.deleteIfExists(tempFile2.toPath());
        }
    }

    @Test
    public void skipSomeBytes() throws IOException {
        InputStream extent = mock(InputStream.class);
        when(endpoint.openInputStream(anyString(), anyLong(), anyLong())).thenReturn(extent);

        long fileSize = 6; // 123456
        long extentStride = 3;
        stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

        long skipLen = extentStride + 1; // skip: 1234
        long skipped = stream.skip(skipLen);
        assertEquals(skipped, skipLen);
        assertEquals(stream.getExtentStartOffset(), skipLen);
        assertEquals(stream.getFileOffset(), skipLen);
    }

    @Test
    public void skipFileSize() throws IOException {
        InputStream extent = mock(InputStream.class);
        when(endpoint.openInputStream(anyString(), anyLong(), anyLong())).thenReturn(extent);

        long fileSize = 6; // 123456
        long extentStride = 3;
        stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

        long skipLen = fileSize; // skip: 123456
        long skipped = stream.skip(skipLen);
        assertEquals(skipped, skipLen);
        assertEquals(stream.getExtentStartOffset(), fileSize);
        assertEquals(stream.getFileOffset(), fileSize);
    }

    @Test
    public void skipTooManyBytes() throws IOException {
        InputStream extent = mock(InputStream.class);
        when(endpoint.openInputStream(anyString(), anyLong(), anyLong())).thenReturn(extent);

        long fileSize = 6; // 123456
        long extentStride = 3;
        stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

        long skipLen = 7; // skip: 1234567, of course 7th byte is one pass the file size 6
        long skipped = stream.skip(skipLen);
        assertEquals(skipped, fileSize);
        assertEquals(stream.getExtentStartOffset(), fileSize);
        assertEquals(stream.getFileOffset(), fileSize);
    }

    @Test
    public void skipTwice() throws IOException {
        InputStream extent = mock(InputStream.class);
        when(endpoint.openInputStream(anyString(), anyLong(), anyLong())).thenReturn(extent);

        long fileSize = 6; // 123456
        long extentStride = 3;
        stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

        long skipped = stream.skip(2);
        assertEquals(skipped, 2);
        assertEquals(stream.getExtentStartOffset(), 2);
        assertEquals(stream.getFileOffset(), 2);

        skipped = stream.skip(2);
        assertEquals(skipped, 2);
        assertEquals(stream.getExtentStartOffset(), 2 + 2);
        assertEquals(stream.getFileOffset(), 2 + 2);
    }

    @Test
    public void randomAccess() throws IOException {
        InputStream extent = mock(InputStream.class);
        when(endpoint.openInputStream(anyString(), anyLong(), anyLong())).thenReturn(extent);

        long fileSize = 6; // 123456
        long extentStride = 3;
        stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

        stream.seek(3);
        assertEquals(stream.getExtentStartOffset(), 3);
        assertEquals(stream.getFileOffset(), 3);

        stream.seek(4);
        assertEquals(stream.getExtentStartOffset(), 4);
        assertEquals(stream.getFileOffset(), 4);

        stream.seek(1);
        assertEquals(stream.getExtentStartOffset(), 1);
        assertEquals(stream.getFileOffset(), 1);

        stream.seek(0);
        assertEquals(stream.getExtentStartOffset(), 0);
        assertEquals(stream.getFileOffset(), 0);

        try {
            stream.seek(-1);
            fail("Should throw at invalid offset");
        } catch (AssertionError expected) {
            // pass
        }

        try {
            stream.seek(fileSize);
            fail("Should throw at invalid offset");
        } catch (AssertionError expected) {
            // pass
        }

        try {
            stream.seek(fileSize + 1);
            fail("Should throw at invalid offset");
        } catch (AssertionError expected) {
            // pass
        }
    }

    @Test
    public void notAvailableWhenEof() throws IOException {
        long fileSize = 0; // empty file
        long extentStride = 3;
        stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);
        assertEquals(stream.available(), 0);
    }

    @Test
    public void availableWhenHavingNextExtent() throws IOException {
        long fileSize = 4; // extent1=3 bytes, extent2=1 byte
        long extentStride = 3;
        long extentSize = extentStride;
        long extentStartOffset = 0;
        long extentPosition = extentSize; // at the end of extent1
        long fileOffset = extentPosition;

        InputStream inputStream = mock(InputStream.class);

        stream = new ExtentInputStream(inputStream, FILE_NAME, fileSize, null, extentStride, extentSize, extentStartOffset, extentPosition, fileOffset);
        assertEquals(stream.available(), 4 - 3);
    }

    @Test
    public void availableWhenExtentAvailable() throws IOException {
        long fileSize = 123;
        long extentStride = 3;

        stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);
        assertEquals(stream.available(), extentStride);
    }

    @Test
    public void readIntSignExention() throws IOException {
        try (var extent = Files.newInputStream(tempFile.toPath())) {
            when(endpoint.openInputStream(anyString(), anyLong(), anyLong())).thenReturn(extent);

            final var out = Files.newOutputStream(tempFile.toPath());
            int v = 5580;
            out.write((v >>> 0) & 0xFF); // 204
            out.write((v >>> 8) & 0xFF); // 21
            out.write((v >>> 16) & 0xFF); // 0
            out.write((v >>> 24) & 0xFF); // 0
            out.flush();
            out.close();

            long fileSize = 4;
            long extentStride = fileSize;

            stream = ExtentInputStream.of(FILE_NAME, fileSize, endpoint, extentStride);

            assertEquals(stream.read(), 204);
            assertEquals(stream.read(), 21);
            assertEquals(stream.read(), 0);
            assertEquals(stream.read(), 0);
            assertEof();
        }
    }
}
