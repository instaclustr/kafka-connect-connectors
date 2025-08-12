package com.instaclustr.kafka.connect.stream;

import com.instaclustr.kafka.connect.stream.endpoint.S3BucketAws;
import com.instaclustr.kafka.connect.stream.endpoint.LocalFile;
import com.instaclustr.kafka.connect.stream.endpoint.S3BucketOntap;
import com.instaclustr.kafka.connect.stream.endpoint.S3BucketStorageGrid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class Endpoints {
    public static final String ENDPOINT_TYPE = "endpoint.type";
    public static final String AWS_S3 = "awss3";
    public static final String LOCAL_FILE = "localfile";
    public static final String ONTAP_S3 = "ontaps3";
    public static final String STORAGEGRID_S3 = "storagegrids3";

    static final ConfigDef CONFIG_DEF = new ConfigDef().define(
            ENDPOINT_TYPE,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.CaseInsensitiveValidString.in(AWS_S3, LOCAL_FILE, ONTAP_S3, STORAGEGRID_S3),
            ConfigDef.Importance.HIGH,
            "Endpoint type: AwsS3, OntapS3, or StorageGridS3");

    public static Endpoint of(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        String endpointType = config.getString(ENDPOINT_TYPE).trim().toLowerCase();
        switch (endpointType) {
            case AWS_S3:
                return S3BucketAws.of(props);
            case LOCAL_FILE:
                return LocalFile.of(props);
            case ONTAP_S3:
                return S3BucketOntap.of(props);
            case STORAGEGRID_S3:
                return S3BucketStorageGrid.of(props);
            default:
                throw new UnsupportedOperationException(endpointType);
        }
    }

}
