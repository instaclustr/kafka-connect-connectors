package com.instaclustr.kafka.connect.sla;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlaSinkConnector extends SinkConnector {
    public static final String FILE_CONFIG = "file";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILE_CONFIG, ConfigDef.Type.STRING, "/opt/kafka/logs/KafkaConnectLatencyMetrics", ConfigDef.Importance.HIGH, "Destination filename.");

    private String filename;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        filename = parsedConfig.getString(FILE_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SlaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (filename != null)
                config.put(FILE_CONFIG, filename);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since SlaSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}
