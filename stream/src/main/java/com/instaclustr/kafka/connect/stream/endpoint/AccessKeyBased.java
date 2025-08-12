package com.instaclustr.kafka.connect.stream.endpoint;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public interface AccessKeyBased {
    String ACCESS_KEY = "access.key";
    String ACCESS_KEY_ID = "access.key.id";

    ConfigDef CONFIG_DEF = new ConfigDef()
                .define(ACCESS_KEY, PASSWORD, HIGH, "Access key")
                .define(ACCESS_KEY_ID, STRING, null, new ConfigDef.NonEmptyStringWithoutControlChars(), HIGH, "Access key id");
}
