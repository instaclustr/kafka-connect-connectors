package com.instaclustr.kafka.connect.s3.source;

import com.instaclustr.kafka.connect.s3.AwsStorageConnectorCommonConfig;
import com.instaclustr.kafka.connect.s3.RegexStringValidator;
import com.instaclustr.kafka.connect.s3.TransferManagerProvider;
import com.instaclustr.kafka.connect.s3.VersionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class AwsStorageSourceConnector extends SourceConnector {
    private static Logger log = LoggerFactory.getLogger(AwsStorageSourceConnector.class);
    public static final String SOURCE_TOPICS = "s3.topics";
    public static final String SINK_TOPIC_PREFIX = "kafka.topicPrefix";
    public static final String MAX_RECORDS_PER_SECOND = "maxRecordsPerSecond";
    public static final String MAX_RECORDS_PER_SECOND_DEFAULT = "500";
    public static final String WORKER_TASK_PARTITIONS_ENTRY = "worker.tasks";
    private ScheduledFuture<?> scheduledFuture;
    private List<String> topicPartitionList;
    private ScheduledExecutorService scheduler;
    private StoredTopicPartitionInfoRetriever storedTopicPartitionInfoRetriever;
    Map<String, String> configMap;

    private TransferManagerProvider transferManagerProvider;


    public AwsStorageSourceConnector() { //do not remove, kafka connect usage
    }

    public AwsStorageSourceConnector(TransferManagerProvider transferManagerProvider, StoredTopicPartitionInfoRetriever storedTopicPartitionInfoRetriever) {
        this.storedTopicPartitionInfoRetriever = storedTopicPartitionInfoRetriever;
        this.transferManagerProvider = transferManagerProvider;
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    private void init(final Map<String, String> configMap) {
        if (this.transferManagerProvider == null) {
            this.transferManagerProvider = new TransferManagerProvider(configMap);
        }
        if (this.storedTopicPartitionInfoRetriever == null) {
            this.storedTopicPartitionInfoRetriever = new StoredTopicPartitionInfoRetriever(configMap.get(AwsStorageConnectorCommonConfig.BUCKET),
                    configMap.getOrDefault(AwsStorageConnectorCommonConfig.S3_KEY_PREFIX, ""),
                    configMap.getOrDefault(SOURCE_TOPICS, "").trim(),
                    transferManagerProvider.get());
        }
        if (this.scheduler == null) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor();
        }
    }

    @Override
    public void start(Map<String, String> map) {
        this.configMap = map;
        this.init(map);
        if (this.topicPartitionList == null) {
            this.topicPartitionList = this.storedTopicPartitionInfoRetriever.getStoredTopicsAndPartitionInfo();
            log.info("Starting s3 source connector with topics : {}", this.topicPartitionList);
        } else {
            log.info("Starting s3 source connector with no topics found");
        }

        this.scheduledFuture = scheduler.scheduleWithFixedDelay(() -> {
            if (areThereNewTopicPartitions()) {
                log.info("Found new topic partitions. Triggering reconfiguration!");
                this.context.requestTaskReconfiguration();
            } else {
                log.info("No new topic partitions discovered. No changes to tasks.");
            }
        }, 5, 5, TimeUnit.MINUTES);
    }


    @Override
    public Class<? extends Task> taskClass() {
        return AwsStorageSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        int requiredTasks = Math.min(this.topicPartitionList.size(), maxTasks);
        Map<String, String> uniqueMap;
        if (!this.topicPartitionList.isEmpty()) {
            List<List<String>> workBreakDown = ConnectorUtils.groupPartitions(topicPartitionList, requiredTasks);
            for (int i = 0; i < requiredTasks; i++) {
                uniqueMap = new HashMap<>(configMap);
                uniqueMap.put(WORKER_TASK_PARTITIONS_ENTRY, StringUtils.join(workBreakDown.get(i), ","));
                configs.add(uniqueMap);
            }
        } else {
            uniqueMap = new HashMap<>(configMap);
            configs.add(uniqueMap);
        }
        return configs;
    }

    private boolean areThereNewTopicPartitions() {
        int currentTopicPartitionCount = this.topicPartitionList.size();
        this.topicPartitionList = this.storedTopicPartitionInfoRetriever.getStoredTopicsAndPartitionInfo();
        return currentTopicPartitionCount < this.topicPartitionList.size();
    }

    @Override
    public void stop() {
        if (scheduledFuture != null && !(scheduledFuture.isCancelled() || scheduledFuture.isDone())) {
            scheduledFuture.cancel(true);
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @Override
    public Config validate(final Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        AwsStorageConnectorCommonConfig.verifyS3CredentialsAndBucketInfo(connectorConfigs, config);
        return config;
    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = AwsStorageConnectorCommonConfig.conf();
        configDef.define(SOURCE_TOPICS, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Define the required topics to be read from s3");
        configDef.define(SINK_TOPIC_PREFIX, ConfigDef.Type.STRING, "",
                new RegexStringValidator(Pattern.compile("^$|^[a-zA-Z0-9]+$"), "Only alphanumeric allowed"),
                ConfigDef.Importance.MEDIUM, "Prefix for the created topics");
        configDef.define(MAX_RECORDS_PER_SECOND, ConfigDef.Type.INT, MAX_RECORDS_PER_SECOND_DEFAULT, ConfigDef.Importance.MEDIUM, "Rate limit the number of records produced per second");
        return configDef;
    }
}
