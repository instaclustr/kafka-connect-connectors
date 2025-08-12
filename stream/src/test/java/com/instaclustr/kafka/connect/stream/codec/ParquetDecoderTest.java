package com.instaclustr.kafka.connect.stream.codec;

import com.instaclustr.kafka.connect.stream.endpoint.ExtentBased;
import com.instaclustr.kafka.connect.stream.endpoint.LocalFile;
import com.instaclustr.kafka.connect.stream.RandomAccessInputStream;
import org.apache.kafka.connect.data.Struct;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.util.*;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.testng.Assert.*;

public class ParquetDecoderTest {
    
    private ParquetDecoder decoder;
    private File uncompressedFile;
    private File compressedFile;

    private static final int NUM_USERS = 1000;
    private static final List<PhoneBookWriter.User> DATA = Collections.unmodifiableList(makeUsers(NUM_USERS));
    private static final Map<String, String> CONFIG = Map.of(ExtentBased.EXTENT_STRIDE, String.valueOf(16l));
    private static LocalFile LOCAL_FILE = LocalFile.of(CONFIG);

    public static List<PhoneBookWriter.User> makeUsers(int rowCount) {
        List<PhoneBookWriter.User> users = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            PhoneBookWriter.Location location = null;
            if (i % 3 == 1) {
                location = new PhoneBookWriter.Location((double) i, (double) i * 2);
            }
            if (i % 3 == 2) {
                location = new PhoneBookWriter.Location((double) i, null);
            }
            Map<String, Double> accounts = null;
            if (i % 3 == 0) {
                accounts = new HashMap<>();
                accounts.put("k1", 1.0);
                accounts.put("k2", 2.0);
            }
            // row index of each row in the file is same as the user id.
            users.add(new PhoneBookWriter.User(
                    i,
                    "p" + i,
                    List.of(new PhoneBookWriter.PhoneNumber(i, "cell")),
                    location,
                    accounts));
        }
        return users;
    }

    public static void writePhoneBookToFile(Path file, ParquetProperties.WriterVersion parquetVersion, CompressionCodecName compressionCodec)
            throws IOException {
            int pageSize = DATA.size() / 10; // Ensure that several pages will be created
            int rowGroupSize = pageSize * 6 * 5; // Ensure that there are more row-groups created

            PhoneBookWriter.write(
                    ExampleParquetWriter.builder(file)
                    .withWriteMode(OVERWRITE)
                    .withRowGroupSize(rowGroupSize)
                    .withPageSize(pageSize)
                    .withWriterVersion(parquetVersion)
                    .withCompressionCodec(compressionCodec),
                    DATA);
    }

    @BeforeMethod
    public void setup() throws IOException {
        uncompressedFile = File.createTempFile("ParquetDecoderTest-uncompressedFile", ".parquet");
        writePhoneBookToFile(new Path(uncompressedFile.getAbsolutePath()), ParquetProperties.WriterVersion.PARQUET_2_0, CompressionCodecName.UNCOMPRESSED);
        compressedFile = File.createTempFile("ParquetDecoderTest-compressedFile", ".parquet");
        writePhoneBookToFile(new Path(compressedFile.getAbsolutePath()), ParquetProperties.WriterVersion.PARQUET_2_0, CompressionCodecName.SNAPPY);

        if (decoder != null) {
            decoder.close();
        }
        decoder = null;
    }

    @AfterMethod
    public void teardown() throws IOException {
        Files.deleteIfExists(uncompressedFile.toPath());
        Files.deleteIfExists(compressedFile.toPath());
        if (decoder != null) {
            decoder.close();
        }
    }

    @Test
    public void decodeOneByOne() throws IOException {
        for (File file : List.of(uncompressedFile, compressedFile)) {
            RandomAccessInputStream rais = LOCAL_FILE.openRandomAccessInputStream(file.getAbsolutePath());
            decoder = ParquetDecoder.from(rais);

            for (PhoneBookWriter.User expected : DATA) {
                List<Record<Struct>> batch = decoder.next(1);
                assertEquals(batch.size(), 1);
                Struct actual = batch.get(0).getRecord();
                checkUser(expected, actual);
                checkRecordMetadata(batch.get(0), expected.equals(DATA.get(DATA.size() - 1)));
            }
        }
    }

    public static void checkUser(final PhoneBookWriter.User expected, final Struct actual) {
        assertEquals(actual.getInt64("id"), Long.valueOf(expected.getId()));
        assertEquals(actual.getString("name"), expected.getName());
        assertTrue(actual.get("phoneNumbers") instanceof Struct);
        List<Struct> phone = actual.getStruct("phoneNumbers").<Struct>getArray("phone");
        assertEquals(phone.size(), expected.getPhoneNumbers().size());
        assertEquals(phone.get(0).get("number"), expected.getPhoneNumbers().get(0).getNumber());
        assertEquals(phone.get(0).getString("kind"), expected.getPhoneNumbers().get(0).getKind());

        if (expected.getLocation() == null) {
            assertNull(actual.getStruct("location"));
        } else {
            assertEquals(actual.getStruct("location").getFloat64("lon"), expected.getLocation().getLon());
            assertEquals(actual.getStruct("location").getFloat64("lat"), expected.getLocation().getLat());
        }

        if (expected.getAccounts() == null) {
            assertNull(actual.getStruct("accounts"));
        } else {
            List<Struct> key_value = actual.getStruct("accounts").getArray("key_value");
            assertEquals(key_value.size(), 2);
            assertEquals(new String(key_value.get(0).getBytes("key")), "k1");
            assertEquals(key_value.get(0).getFloat64("value"), Double.valueOf(1.0));
            assertEquals(new String(key_value.get(1).getBytes("key")), "k2");
            assertEquals(key_value.get(1).getFloat64("value"), Double.valueOf(2.0));
        }
    }

    @Test
    public void decodeBatch() throws IOException {
        RandomAccessInputStream rais = LOCAL_FILE.openRandomAccessInputStream(uncompressedFile.getAbsolutePath());
        decoder = ParquetDecoder.from(rais);

        int batchSize = 257;
        for (int i = 0; i < DATA.size(); i+=batchSize) {
            List<Record<Struct>> batch = decoder.next(batchSize);
            if (i + batchSize <= NUM_USERS) {
                assertEquals(batch.size(), batchSize);
            } else {
                assertTrue(batch.size() < batchSize);
            }

            for (int j = 0; j < batch.size(); j++) {
                Struct actual = batch.get(j).getRecord();
                PhoneBookWriter.User expected = DATA.get(i + j);
                checkUser(expected, actual);
                checkRecordMetadata(batch.get(j), i + j == DATA.size() - 1);
                
            }
        }
    }

    public static void checkRecordMetadata(Record<Struct> record, boolean isLastRecord) {
        assertNotNull(record.getSchema());
        assertNull(record.getStreamOffset());
        assertNotNull(record.getStreamProgress());
        if (isLastRecord) {
            // last record
            assertEquals(record.getStreamProgress(), Double.valueOf(1));
        } else {
            assertTrue(0 < record.getStreamProgress() && record.getStreamProgress() < 1.0);
        }
    }
    
    @Test
    public void writerVersion1() throws IOException {
        compressedFile = File.createTempFile("ParquetDecoderTest-compressedFile-v1", ".parquet");
        writePhoneBookToFile(new Path(uncompressedFile.getAbsolutePath()), ParquetProperties.WriterVersion.PARQUET_1_0, CompressionCodecName.ZSTD);
        decodeBatch();
    }
}
