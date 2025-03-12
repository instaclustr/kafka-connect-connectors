package com.instaclustr.kafka.connect.s3.sink;

import com.instaclustr.kafka.connect.s3.AwsStorageConnectorCommonConfig;
import com.instaclustr.kafka.connect.s3.VersionUtil;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AwsStorageSinkConnector extends SinkConnector {
  private static Logger logger = LoggerFactory.getLogger(AwsStorageSinkConnector.class);
  private Map<String, String> configMap;

  public AwsStorageSinkConnector() { // do not remove, kafka connect usage
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    this.configMap = map;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AwsStorageSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> uniqueMap = new HashMap<>(configMap);
      uniqueMap.put("task.id", ""+i);
      configs.add(uniqueMap);
    }
    return configs;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public Config validate(final Map<String, String> connectorConfigs) {
    Config config = super.validate(connectorConfigs);
    AwsStorageConnectorCommonConfig.verifyS3CredentialsAndBucketInfo(connectorConfigs, config);
    return config;
  }

  @Override
  public void stop() {
    logger.info("Stopping AwsStorageSinkConnector");
  }

  @Override
  public ConfigDef config() {
    return AwsStorageConnectorCommonConfig.conf();
  }
}
