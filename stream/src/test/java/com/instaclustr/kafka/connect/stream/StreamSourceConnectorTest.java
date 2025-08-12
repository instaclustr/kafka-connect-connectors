package com.instaclustr.kafka.connect.stream;

import static com.instaclustr.kafka.connect.stream.StreamSourceConnector.*;
import static java.lang.Thread.sleep;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.instaclustr.kafka.connect.stream.codec.CharDecoder;
import com.instaclustr.kafka.connect.stream.codec.Decoders;
import com.instaclustr.kafka.connect.stream.endpoint.AccessKeyBased;
import com.instaclustr.kafka.connect.stream.endpoint.ExtentBased;
import com.instaclustr.kafka.connect.stream.endpoint.S3Bucket;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StreamSourceConnectorTest {
    private File dir;
    private Set<String> files;
    private Map<String, String> config;
    private StreamSourceConnector connector;
    
    @BeforeMethod
    public void setup() throws IOException {
        dir = Files.createTempDirectory("connector-test-method").toFile();
        dir.deleteOnExit();
        files = new HashSet<>();
        files.addAll(List.of(
                File.createTempFile("connector-test-file1", null, dir).getAbsolutePath(),
                File.createTempFile("connector-test-file2", null, dir).getAbsolutePath()));

        config = new HashMap<>();
        config.put(StreamSourceTask.TOPIC_CONFIG, "dummyTopic");
        config.put(Endpoints.ENDPOINT_TYPE, Endpoints.LOCAL_FILE);
        
        connector = new StreamSourceConnector();
    }
    
    @AfterMethod
    public void tearDown() {
        connector.stop();
    }

    @Test
    public void correctConfig() {
        var correctConfig = new HashMap<String, String>();
        correctConfig.put(AccessKeyBased.ACCESS_KEY_ID, "myKeyId");
        correctConfig.put(AccessKeyBased.ACCESS_KEY, "myKey");
        correctConfig.put(Decoders.DECODER_TYPE, Decoders.TEXT);
        correctConfig.put(CharDecoder.CHARACTER_SET, Charset.forName("utf-8").name());
        correctConfig.put(Endpoints.ENDPOINT_TYPE, Endpoints.STORAGEGRID_S3);
        correctConfig.put(S3Bucket.BUCKET_NAME, "myBucketName");
        correctConfig.put(S3Bucket.REGION, "myRegion");
        correctConfig.put(S3Bucket.URL, "myUrl");
        correctConfig.put(StreamSourceTask.TOPIC_CONFIG, "myTopic");
        correctConfig.put(StreamSourceConnector.FILES_CONFIG, "myFile");

        var validatedConfig = connector.validate(correctConfig);
        List<String> errors = new LinkedList<>();
        for (ConfigValue v : validatedConfig.configValues()) {
            if (! v.errorMessages().isEmpty()) {
                errors.addAll(v.errorMessages());
            }
        }
        assertTrue(errors.isEmpty());
    }

    @Test
    public void defineDirectoryXorFiles() {
        // configure
        config.put(Decoders.DECODER_TYPE, Decoders.PARQUET);

        // null files and directory -- error
        var result = connector.validate(config);
        assertErrorWith(result, FILES_CONFIG, ERROR_FILES_XOR_DIRECTORY);
        assertErrorWith(result, DIRECTORY_CONFIG, ERROR_FILES_XOR_DIRECTORY);

        // only directory -- okay
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        result = connector.validate(config);
        assertNoError(result, FILES_CONFIG);
        assertNoError(result, DIRECTORY_CONFIG);

        // both files and directory -- error
        config.put(FILES_CONFIG, String.join(",", files));
        result = connector.validate(config);
        assertErrorWith(result, FILES_CONFIG, ERROR_FILES_XOR_DIRECTORY);
        assertErrorWith(result, DIRECTORY_CONFIG, ERROR_FILES_XOR_DIRECTORY);

        // only files -- okey
        config.remove(DIRECTORY_CONFIG);
        result = connector.validate(config);
        assertNoError(result, FILES_CONFIG);
        assertNoError(result, DIRECTORY_CONFIG);
    }

    private static void assertErrorWith(final Config result, final String configName, final String errorKeywords) {
        List<String> errors = result.configValues().stream().filter(cv -> cv.name().equals(configName)).flatMap(cv -> cv.errorMessages().stream()).collect(Collectors.toList());
        assertEquals(errors.size(), 1);
        assertEquals(errors.get(0), errorKeywords);
    }

    private static void assertNoError(final Config result, final String configName) {
        List<String> errors = result.configValues().stream().filter(cv -> cv.name().equals(configName)).flatMap(cv -> cv.errorMessages().stream()).collect(Collectors.toList());
        assertTrue(errors.isEmpty());
    }

    @Test
    public void testDirectory() throws IOException {
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        connector.start(config);

        assertEquals(Set.copyOf(connector.getFiles()), files);
    }
    
    @Test
    public void testFiles() throws IOException {
        config.put(FILES_CONFIG, String.join(",", files));
        connector.start(config);

        assertEquals(Set.copyOf(connector.getFiles()), files);
    }
    
    @Test(expectedExceptions = {ConnectException.class})
    public void shouldNotDefineBothDirectoryAndFiles() throws IOException {
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        config.put(FILES_CONFIG, String.join(",", files));
        connector.start(config);
    }

    @Test(expectedExceptions = {ConnectException.class})
    public void shouldNotMissDirectoryAndFiles() throws IOException {
        connector.start(config);
    }
    
    @Test
    public void testFileDiscovery() throws IOException, InterruptedException {
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        connector = Mockito.spy(connector);
        doReturn(Duration.ofSeconds(1)).when(connector).getDirectoryFileDiscoveryDuration();
        var context = mock(SourceConnectorContext.class);
        doReturn(context).when(connector).getContext();
        connector.start(config);
        assertEquals(Set.copyOf(connector.getFiles()), files);
        
        sleep(1500);
        verify(context, times(0)).requestTaskReconfiguration();
        files.add(File.createTempFile("file3", null, dir).getAbsolutePath());
        sleep(1000);;
        verify(context, times(1)).requestTaskReconfiguration();
        assertEquals(Set.copyOf(connector.getFiles()), files);
    }
    
    @Test
    public void testTaskConfigs() {
        config.put(StreamSourceConnector.DIRECTORY_CONFIG, dir.getAbsolutePath());
        connector.start(config);

        var tasks = connector.taskConfigs(1);
        assertEquals(tasks.size(), 1);
        
        var actualFiles = Arrays.asList(tasks.get(0).get(StreamSourceTask.TASK_FILES).split(","));
        assertEquals(Set.copyOf(actualFiles), files);
        
        tasks = connector.taskConfigs(123);
        assertEquals(tasks.size(), 1);
        actualFiles = Arrays.asList(tasks.get(0).get(StreamSourceTask.TASK_FILES).split(","));
        assertEquals(Set.copyOf(actualFiles), files);
    }
}
