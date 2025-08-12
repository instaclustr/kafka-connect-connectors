package com.instaclustr.kafka.connect.stream.codec;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class CharDecoderTest {

    private CharDecoder decoder;
    private File tempFile;

    @BeforeMethod
    public void setup() throws IOException {
        tempFile = File.createTempFile("CharDecoderTest-tempFile", null);
    }

    @AfterMethod
    public void teardown() throws IOException {
        Files.deleteIfExists(tempFile.toPath());
        if (decoder != null) {
            decoder.close();
        }
    }

    @Test
    public void testEmptyLine() throws IOException {
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset);

        String line = "\nfoo\n";
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(line.getBytes(charset));
            os.flush();
        }

        List<Record<String>> records = decoder.next(2);
        assertEquals(records.size(), 2);

        Record<String> record = records.get(0);
        assertEquals(record.getRecord(), "");
        assertEquals(record.getStreamOffset(), Long.valueOf(1));

        record = records.get(1);
        assertEquals(record.getRecord(), "foo");
        assertEquals(record.getStreamOffset(), Long.valueOf(5));
    }

    @Test
    public void testNonTerminatedLine() throws IOException {
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset);

        String line = "foo";
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(line.getBytes(charset));
            os.flush();
        }

        // First read has available input but not enough to decode a line
        List<Record<String>> records = decoder.next(2);
        assertNotNull(records);
        assertTrue(records.isEmpty());

        // Second read reaches EOF
        records = decoder.next(2);
        assertNull(records);
    }

    @Test
    public void testSeek() throws IOException {
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset);

        String line = "first line\n";
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(line.getBytes(charset));
            os.flush();
        }

        int expectedOneCharInBytes = 1;
        int skipOneChar = 1;
        int skipOneCharInBytes = line.substring(0, skipOneChar).getBytes(charset).length;
        assertEquals(skipOneCharInBytes, expectedOneCharInBytes);
        decoder.skipFirstBytes(skipOneCharInBytes);

        List<Record<String>> records = decoder.next(1);
        assertEquals(records.size(), 1);

        Record<String> record = records.get(0);
        String expectedRecord = "irst line";
        assertEquals(record.getRecord(), expectedRecord);
        assertEquals(record.getStreamOffset(), Long.valueOf(line.getBytes(charset).length));
    }
    
    @Test
    public void testSeekUtf16() throws IOException {
        Charset charset = StandardCharsets.UTF_16;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset);

        String content = "first line\nsecond line\n";
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(content.getBytes(charset));
            os.flush();
        }

        List<Record<String>> records = decoder.next(1);
        assertNotNull(records);
        assertEquals(records.size(), 1);

        Record<String> record = records.get(0);
        assertEquals(record.getRecord(), "first line");
        assertEquals(record.getStreamOffset(), Long.valueOf(2 + "first line\n".length() * 2));
        
        try (CharDecoder decoder2 = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset)) {
			decoder2.skipFirstBytes(record.getStreamOffset());
			records = decoder2.next(1);
			assertNotNull(records);
			assertEquals(records.size(), 1);
			record = records.get(0);
			assertEquals(record.getRecord(), "second line");
			assertEquals(record.getStreamOffset(), Long.valueOf(2 + content.length() * 2));
        }
    }

    @Test
    public void testBatchSize() throws IOException {
        int batchSize = 5000;
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset, 2);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            writeTimesAndFlush(os, 10_000,
                    "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...\n".getBytes()
            );

            assertEquals(decoder.bufferSize(), 2);
            List<Record<String>> records = decoder.next(batchSize);
            assertEquals(records.size(), 5000);
            assertEquals(decoder.bufferSize(), 128);

            records = decoder.next(batchSize);
            assertEquals(records.size(), 5000);
            assertEquals(decoder.bufferSize(), 128);
        }
    }

    @Test
    public void testBufferResize() throws IOException {
        int batchSize = 1000;
        Charset charset = StandardCharsets.UTF_8;
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), charset, 2);

        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            assertEquals(decoder.bufferSize(), 2);

            writeAndAssertBufferSize(batchSize, os, "1\n".getBytes(charset), 2);
            writeAndAssertBufferSize(batchSize, os, "333\n".getBytes(charset), 4);
            writeAndAssertBufferSize(batchSize, os, "66666\n".getBytes(charset), 8);
            writeAndAssertBufferSize(batchSize, os, "7777777\n".getBytes(charset), 8);
            writeAndAssertBufferSize(batchSize, os, "88888888\n".getBytes(charset), 16);
            writeAndAssertBufferSize(batchSize, os, "999999999\n".getBytes(charset), 16);

            byte[] bytes = new byte[1025];
            Arrays.fill(bytes, (byte) '*');
            bytes[1024] = '\n';
            writeAndAssertBufferSize(batchSize, os, bytes,2048);
            writeAndAssertBufferSize(batchSize, os, "4444\n".getBytes(charset),2048);
        }
    }

    @Test
    public void testUtf16() throws IOException {
        Map<String, String> props = new HashMap<>();
        props.put(CharDecoder.CHARACTER_SET, "UTF-16");
        decoder = CharDecoder.of(Files.newInputStream(tempFile.toPath()), props);

        Charset expectedCharset = StandardCharsets.UTF_16;
        assertEquals(decoder.getCharset(), expectedCharset);

        String line = "foo\nbar\n";
        byte[] lineBytes = line.getBytes(expectedCharset);
        try (OutputStream os = Files.newOutputStream(tempFile.toPath())) {
            os.write(lineBytes);
            os.flush();
        }

        List<Record<String>> records = decoder.next(2);
        assertEquals(records.size(), 2);
        assertEquals(records.get(0).getRecord(), "foo");
        assertEquals(records.get(0).getStreamOffset(), Long.valueOf(2 + 4 * 2));
        assertEquals(records.get(1).getRecord(), "bar");
        assertEquals(records.get(1).getStreamOffset(), Long.valueOf(2 + 4 * 2 + 4 * 2));
    }

    @Test
    public void testAlwaysReadyReader() throws IOException {
        // This is the case for AWS S3 inputstream over HTTP connection
        // EOF is detected by read() returning -1

        Reader reader = mock(Reader.class);
        when(reader.ready()).thenReturn(true);
        when(reader.read(any(), anyInt(), anyInt())).thenReturn(-1);

        char[] buffer = new char[2];
        CharDecoder decoder = new CharDecoder(null, 0, reader, null, buffer, 0);
        List<Record<String>> result = decoder.next(1);
        assertNull(result); // EOF

        when(reader.read(any(), anyInt(), anyInt()))
                .thenReturn(1)
                .thenReturn(-1);
        buffer[0] = 'a';
        result = decoder.next(1);
        assertTrue(result.isEmpty()); // Not enough input

        result = decoder.next(1);
        assertNull(result); // EOF
    }

    @Test
    public void testNotReadyReader() throws IOException {
        // This is the case for local file
        Reader reader = mock(Reader.class);
        when(reader.ready()).thenReturn(false);

        char[] buffer = new char[2];
        CharDecoder decoder = new CharDecoder(null, 0, reader, null, buffer, 0);
        List<Record<String>> result = decoder.next(1);
        assertNull(result); // EOF

        when(reader.ready())
                .thenReturn(true)
                .thenReturn(false);
        when(reader.read(any(), anyInt(), anyInt()))
                .thenReturn(1)
                .thenReturn(-1);
        buffer[0] = 'a';
        result = decoder.next(1);
        assertTrue(result.isEmpty()); // Not enough input

        result = decoder.next(1);
        assertNull(result); // EOF
    }

    @Test
    public void testNotEnoughStream() throws IOException {
        Reader reader = mock(Reader.class);
        when(reader.ready()).thenReturn(true);
        when(reader.read(any(), anyInt(), anyInt())).thenReturn(0);

        char[] buffer = new char[2];
        CharDecoder decoder = new CharDecoder(null, 0, reader, null, buffer, 0);
        List<Record<String>> result = decoder.next(1);
        assertNull(result); // EOF

        when(reader.read(any(), anyInt(), anyInt()))
                .thenReturn(1)
                .thenReturn(0);
        buffer[0] = 'a';
        result = decoder.next(1);
        assertTrue(result.isEmpty()); // Not enough input

        result = decoder.next(1);
        assertNull(result); // EOF
    }

    @Test
    public void testAppend() throws IOException {
        Reader reader = mock(Reader.class);
        when(reader.ready()).thenReturn(true);

        char[] buffer = new char[2];
        CharDecoder decoder = new CharDecoder(null, 0, reader, StandardCharsets.UTF_8, buffer, 0);
        when(reader.read(any(), anyInt(), anyInt()))
                .thenReturn(1)
                .thenReturn(-1);
        buffer[0] = 'a';
        List<Record<String>> result = decoder.next(2);
        assertTrue(result.isEmpty()); // Not enough input
        assertEquals(decoder.getStreamOffset(), 0);

        // Append
        when(reader.read(any(), anyInt(), anyInt()))
                .thenReturn(1)
                .thenReturn(-1);
        buffer[1] = '\n';
        result = decoder.next(2);
        assertNotNull(result); // EOF
        assertEquals(result.get(0).getRecord(), "a");

        // Not append
        when(reader.read(any(), anyInt(), anyInt())).thenReturn(-1);
        result = decoder.next(2);
        assertNull(result);
    }

    @Test
    public void testCharsetBytes() throws CodecError {
        Charset charset = StandardCharsets.UTF_8;
        char[] cb = new char[] { 'a', 'b', 'c' };
        int result = CharDecoder.numBytesOf(charset, cb, 1, 2);
        assertEquals(result, 2);

        charset = StandardCharsets.UTF_16;
        result = CharDecoder.numBytesOf(charset, cb, 1, 2);
        assertEquals(result, 2 * 2);
    }

    @Test
    public void testMoreCharacters() throws CodecError {
        Charset charset = StandardCharsets.UTF_8;
        char[] cb = new char[] { 0x79, 0x799, Character.MIN_SURROGATE, Character.MIN_SURROGATE - 1 };
        assertEquals(CharDecoder.numBytesOf(charset, cb, 0, 1), 1);
        assertEquals(CharDecoder.numBytesOf(charset, cb, 1, 1), 2);
        assertEquals(CharDecoder.numBytesOf(charset, cb, 2, 1), 4);
        assertEquals(CharDecoder.numBytesOf(charset, cb, 3, 1), 3);

        charset = StandardCharsets.UTF_16;
        int byteOrderMarkBytes = 2;
        for (int i = 0; i < cb.length; i++) {
            assertEquals(CharDecoder.numBytesOf(charset, cb, i, 1),
                    charset.encode(CharBuffer.wrap(cb, i, 1)).limit() - byteOrderMarkBytes);
        }
    }

    private void writeAndAssertBufferSize(int batchSize, OutputStream os, byte[] bytes, int expectBufferSize)
            throws IOException {
        writeTimesAndFlush(os, batchSize, bytes);
        List<Record<String>> records = decoder.next(batchSize);
        assertEquals(batchSize, records.size());
        String expectedLine = new String(bytes, 0, bytes.length - 1); // remove \n
        for (Record<String> record : records) {
            assertEquals(record.getRecord(), expectedLine);
        }
        assertEquals(decoder.bufferSize(), expectBufferSize);
    }

    private void writeTimesAndFlush(OutputStream os, int times, byte[] line) throws IOException {
        for (int i = 0; i < times; i++) {
            os.write(line);
        }
        os.flush();
    }
}
