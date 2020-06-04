package com.instaclustr.kafka.connect.sla;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class VersionUtil {

    private static Logger log = LoggerFactory.getLogger(VersionUtil.class);

    private VersionUtil() {
    }
    @SuppressWarnings("Duplicates")
    public static String getVersion() {
        final String groupId = "com.instaclustr.kafkaconnect";
        final String artifactId = "instaclustr-sla-connector";
        try (InputStream inputStream = VersionUtil.class.getResourceAsStream(String.format("/META-INF/maven/%s/%s/pom.properties", groupId, artifactId))) {
            Properties pomProperties = new Properties();
            pomProperties.load(inputStream);
            return pomProperties.getProperty("version");
        } catch (Exception ex) {
            log.warn("Could not read the version from pom.properties", ex);
            return null;
        }
    }
}
