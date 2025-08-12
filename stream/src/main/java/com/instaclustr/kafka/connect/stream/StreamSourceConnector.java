package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.codec.CharDecoder;
import com.instaclustr.kafka.connect.stream.codec.CodecError;
import com.instaclustr.kafka.connect.stream.codec.Decoders;
import com.instaclustr.kafka.connect.stream.endpoint.AccessKeyBased;
import com.instaclustr.kafka.connect.stream.endpoint.ExtentBased;
import com.instaclustr.kafka.connect.stream.endpoint.S3Bucket;
import org.apache.kafka.common.config.*;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class StreamSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(StreamSourceConnector.class);

    public static final String DIRECTORY_CONFIG = "directory";
    public static final String DIRECTORY_FILE_DISCOVERY_MINUTES = "directory.file.discovery.minutes";
    public static final String FILES_CONFIG = "files";

    public static final long DEFAULT_DIRECTORY_FILE_DISCOVERY_MINUTES = 60;
    public static final String ERROR_FILES_XOR_DIRECTORY = "Should define either " + FILES_CONFIG + " xor " + DIRECTORY_CONFIG;

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILES_CONFIG,
                    ConfigDef.Type.LIST,
                    null,
                    (name, value) -> {
                        if (value == null) {
                            return;
                        }
                        if (!(value instanceof List)) {
                            throw new ConfigException(name + ": Not a list");
                        }
                        for (Object v : (List) value) {
                            new ConfigDef.NonEmptyStringWithoutControlChars().ensureValid(name, v);
                        }
                    },
                    ConfigDef.Importance.HIGH,
                    "Source filenames, default to files under the same directory.")
            .define(DIRECTORY_CONFIG,
                    ConfigDef.Type.STRING,
                    null,
                    new ConfigDef.NonEmptyStringWithoutControlChars(),
                    ConfigDef.Importance.HIGH,
                    "Source directory.")
            .define(DIRECTORY_FILE_DISCOVERY_MINUTES,
                    ConfigDef.Type.LONG,
                    DEFAULT_DIRECTORY_FILE_DISCOVERY_MINUTES,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.LOW,
                    "The time to discover new directory files");

    private AbstractConfig config;
    
    // Mutable state
    private volatile List<String> files;
    private Watcher directoryWatcher = null;

    @Override
    public void start(final Map<String, String> props) {
        config = new AbstractConfig(CONFIG_DEF, props);
        mustDefineDirectoryXorFiles(config);
        if ((files = config.getList(FILES_CONFIG)) == null) {
            Endpoint endpoint = Endpoints.of(props);
            String directory = config.getString(DIRECTORY_CONFIG);
            try {
                files = endpoint.listRegularFiles(directory).collect(toList());
            } catch (IOException e) {
                throw new ConnectException(e);
            }
            var knownFiles = Set.copyOf(files);
            directoryWatcher = Watcher.of(getDirectoryFileDiscoveryDuration());
            directoryWatcher.watch(() -> {
                var currentFiles = endpoint.listRegularFiles(directory).collect(toList());
                if (! knownFiles.containsAll(currentFiles)) {
                    files = currentFiles;
                    return true;
                }
                return false;
            }, () -> {
                log.info("Files under the directory has changed, request to reconfigure tasks");
                getContext().requestTaskReconfiguration();
            });
        }
        if (files.isEmpty()) {
            throw new ConnectException("Unable to find files to read, files: " + config.getString(FILES_CONFIG) + ", directory: " + config.getString(DIRECTORY_CONFIG));
        }
        log.info("Started stream source connector reading from {}", files);

    }

    private void mustDefineDirectoryXorFiles(AbstractConfig config) {
        if (! hasDefinedDirectoryXorFiles(config)) {
            throw new ConnectException("Files or directory should be configured exclusively");
        }
    }

    private boolean hasDefinedDirectoryXorFiles(AbstractConfig config) {
       return config.getString(DIRECTORY_CONFIG) == null ^ config.getList(FILES_CONFIG) == null;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        List<Map<String, String>> result = new ArrayList<>();
        Map<String, String> copy = config.originalsStrings();
        copy.put(StreamSourceTask.TASK_FILES, String.join(",", files));
        result.add(copy);
        return result;
    }

    @Override
    public void stop() {
        log.info("Stopping");
        if (directoryWatcher != null) {
            try {
                directoryWatcher.close();
            } catch (IOException e) {
                throw new ConnectException("Error while closing directory watcher threads", e);
            } finally {
                directoryWatcher = null;
            }
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config result = super.validate(connectorConfigs);

        List<ConfigValue> otherValidated = Stream.of(
                        AccessKeyBased.CONFIG_DEF.validate(connectorConfigs),
                        CharDecoder.CONFIG_DEF.validate(connectorConfigs),
                        Decoders.CONFIG_DEF.validate(connectorConfigs),
                        Endpoints.CONFIG_DEF.validate(connectorConfigs),
                        ExtentBased.CONFIG_DEF.validate(connectorConfigs),
                        StreamSourceTask.CONFIG_DEF.validate(connectorConfigs),
                        S3Bucket.CONFIG_DEF.validate(connectorConfigs))
                .map(Collection::stream)
                .reduce(Stream.empty(), Stream::concat)
                .collect(toList());

        result.configValues().addAll(otherValidated);

        AbstractConfig config = new AbstractConfig(CONFIG_DEF, connectorConfigs);
        if (! hasDefinedDirectoryXorFiles(config)) {
            result.configValues()
                    .stream()
                    .filter(cv -> cv.name().equals(FILES_CONFIG) || cv.name().equals(DIRECTORY_CONFIG))
                    .forEach(cv -> cv.addErrorMessage(ERROR_FILES_XOR_DIRECTORY));
        }

        return result;
    }

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        return Decoders.exactlyOnceSupportOf(props);
    }

    @Override
    public boolean alterOffsets(Map<String, String> connectorConfig, Map<Map<String, ?>, Map<String, ?>> offsets) {
        AbstractConfig validateOnly = new AbstractConfig(CONFIG_DEF, connectorConfig);

        for (Map.Entry<Map<String, ?>, Map<String, ?>> partitionOffset : offsets.entrySet()) {
            Map<String, ?> offset = partitionOffset.getValue();
            if (offset == null) {
                continue;
            }

            if (!offset.containsKey(StreamSourceTask.POSITION_FIELD)) {
                throw new ConnectException("Offset objects should either be null or contain the key '" + StreamSourceTask.POSITION_FIELD + "'");
            }

            if (!(offset.get(StreamSourceTask.POSITION_FIELD) instanceof Long)) {
                throw new ConnectException("The value for the '" + StreamSourceTask.POSITION_FIELD + "' key in the offset is expected to be a Long value");
            }

            long offsetPosition = (Long) offset.get(StreamSourceTask.POSITION_FIELD);
            if (offsetPosition < 0) {
                throw new ConnectException("The value for the '" + StreamSourceTask.POSITION_FIELD + "' key in the offset should be a non-negative value");
            }

            Map<String, ?> partition = partitionOffset.getKey();
            if (partition == null) {
                throw new ConnectException("Partition objects cannot be null");
            }

            if (!partition.containsKey(StreamSourceTask.FILENAME_FIELD)) {
                throw new ConnectException("Partition objects should contain the key '" + StreamSourceTask.FILENAME_FIELD + "'");
            }
        }

        return true;
    }
    
    // Visible for testing
    List<String> getFiles() {
        return files;
    }
    
    Duration getDirectoryFileDiscoveryDuration() {
        return Duration.ofMinutes(config.getLong(DIRECTORY_FILE_DISCOVERY_MINUTES));
    }
    
    SourceConnectorContext getContext() {
        return context();
    }
}
