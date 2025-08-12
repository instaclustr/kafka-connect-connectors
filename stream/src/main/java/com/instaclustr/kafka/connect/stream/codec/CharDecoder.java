package com.instaclustr.kafka.connect.stream.codec;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CharDecoder implements Decoder<String> {
    public static final String CHARACTER_SET = "character.set";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CHARACTER_SET,
                    ConfigDef.Type.STRING,
                    "UTF-8",
                    ConfigDef.CaseInsensitiveValidString.in("UTF-8", "UTF-16"),
                    ConfigDef.Importance.HIGH,
                    "Character set for text format");

    private static final Logger log = LoggerFactory.getLogger(CharDecoder.class);

    // byte oriented
    private final InputStream stream;
    private long streamOffset;

    // character oriented
    private final Charset charset;
    private final Reader reader;
    private char[] buffer;
    private int offset;

    private static final int INIT_BUFFER_SIZE = 1024;
    private static final Set<Charset> SUPPORTED_CHARSETS = Set.of(StandardCharsets.UTF_8, StandardCharsets.UTF_16);

    CharDecoder(InputStream stream, long streamOffset,
                Reader reader, Charset charset, char[] buffer, int offset) {
        this.stream = stream;
        this.streamOffset = streamOffset;

        this.charset = charset;
        this.reader = reader;
        this.buffer = buffer;
        this.offset = offset;
    }

    public static CharDecoder of(InputStream stream, Map<String, String> config) throws CodecError {
        AbstractConfig validConfig = new AbstractConfig(CONFIG_DEF, config);
        Charset charset = Charset.forName(validConfig.getString(CHARACTER_SET));
        return of(stream, charset);
    }

    public static CharDecoder of(InputStream stream, Charset charset) throws CodecError {
        return of(stream, charset, INIT_BUFFER_SIZE);
    }

    static CharDecoder of(InputStream stream, Charset charset, int initBufferSize) throws CodecError {
        if (!SUPPORTED_CHARSETS.contains(charset)) {
            throw new CodecError("Unsupported: " + charset.name());
        }
        Reader reader = new BufferedReader(new InputStreamReader(stream, charset));
        char[] buffer = new char[initBufferSize];
        return new CharDecoder(stream, byteMarkLengthOf(charset),
                reader, charset, buffer, 0);
    }

    @Override
    public void skipFirstBytes(final long numBytes) throws IOException {
        long skipLeft = numBytes;
        while (skipLeft > 0) {
            long skipped = stream.skip(skipLeft);
            skipLeft -= skipped;
        }
        streamOffset = numBytes;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    @Override
    public List<Record<String>> next(int batchSize) throws IOException {
        List<Record<String>> records = null;
        int nread = 0;
        // Reader over S3's stream appears to ready() == true even when EOF
        // Local filesystem however is !ready() when EOF
        // So we need to test both conditions here for EOF
        while (reader.ready()) {
            nread = reader.read(buffer, offset, buffer.length - offset);
            log.debug("Read {} characters from stream", nread);
            if (nread == -1) {
                // End of stream
                break;
            } else if (nread == 0) {
                // No character decoded from stream input
                break;
            }
            // Must have nread > 0 due to constrained return value range of BufferedReader.read()
            offset += nread;
            if (records == null)
                records = new ArrayList<>();

            String line;
            boolean foundOneLine = false;
            do {
                line = extractLine();
                if (line != null) {
                    foundOneLine = true;
                    log.debug("Extracted a line from buffer");
                    records.add(new Record<>(line, streamOffset, null, Schema.STRING_SCHEMA));

                    if (records.size() >= batchSize) {
                        log.debug("Return records in full batch: size=" + records.size());
                        return records;
                    }
                }
            } while (line != null);

            if (!foundOneLine && offset == buffer.length) {
                char[] newbuf = new char[buffer.length * 2];
                System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
                log.info("Increased buffer from {} to {}", buffer.length, newbuf.length);
                buffer = newbuf;
            }
        }
        return records;
    }

    private String extractLine() throws IOException {
        int until = -1, newStart = -1;
        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                // Check for \r\n to skip, so we must skip this if we can't check the next char
                if (i + 1 >= offset)
                    return null;

                until = i;
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(buffer, 0, until); // convert to UTF16
            int bytesUntilNewStart = numBytesOf(charset, buffer, 0, newStart);

            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;
            streamOffset += bytesUntilNewStart;
            return result;
        } else {
            return null;
        }
    }

    private static int byteMarkLengthOf(Charset charset) throws CodecError {
        if (charset.equals(StandardCharsets.UTF_8)) {
            return 0;
        } else if (charset.equals(StandardCharsets.UTF_16)) {
            return 2;
        } else {
            throw new CodecError("Unsupported: " + charset.name());
        }
    }

    static int numBytesOf(Charset charset, char[] cb, int offset, int length) throws CodecError {
        if (charset.equals(StandardCharsets.UTF_8)) {
            int result = 0;
            for (int i = 0; i < length; i++) {
                result += numBytesOfUtf8Char(cb[offset + i]);
            }
            return result;
        } else if (charset.equals(StandardCharsets.UTF_16)) {
            return length * 2;
        } else {
            throw new CodecError("Unsupported: " + charset.name());
        }
    }

    static int numBytesOfUtf8Char(char c) {
        if (c < 0x80) {
            // Have at most seven bits
            return 1;
        } else if (c < 0x800) {
            // 2 bytes, 11 bits
            return 2;
        } else if (Character.isSurrogate(c)) {
            // Have a surrogate pair
            return 4; // 2 chars
        } else {
            // 3 bytes, 16 bits
            return 3;
        }
    }

    // visible for testing
    int bufferSize() {
        return buffer.length;
    }

    Charset getCharset() {
        return charset;
    }

    long getStreamOffset() {
        return streamOffset;
    }
}
