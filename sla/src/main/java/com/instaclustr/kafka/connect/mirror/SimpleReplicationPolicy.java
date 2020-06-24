package com.instaclustr.kafka.connect.mirror;

import org.apache.kafka.connect.mirror.ReplicationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Defines remote topics without renaming them. */
public class SimpleReplicationPolicy implements ReplicationPolicy {
    private static final Logger log = LoggerFactory.getLogger(SimpleReplicationPolicy.class);

    @Override
    public String formatRemoteTopic(String sourceClusterAlias, String topic) {
        return topic;
    }

    @Override
    public String topicSource(String topic) {
        return null;
    }

    @Override
    public String upstreamTopic(String topic) {
        return null;
    }
}

