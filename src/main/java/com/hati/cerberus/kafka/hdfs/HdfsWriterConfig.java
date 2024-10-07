package com.hati.cerberus.kafka.hdfs;


import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class HdfsWriterConfig extends HdfsSinkConnectorConfig {
    public static final String SOURCE = "source";

    public static ConfigDef newConfigDef() {
        ConfigDef configDef = HdfsSinkConnectorConfig.newConfigDef();
        return extend(configDef);
    }

    private static ConfigDef extend(ConfigDef configDef) {
        configDef.define(SOURCE, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "Source override for topic name.");
        return configDef;
    }

    public static ConfigDef getConfig() {
        ConfigDef config = HdfsSinkConnectorConfig.getConfig();
        return extend(config);
    }

    public HdfsWriterConfig(Map<String, String> props) {
        super(newConfigDef(), addDefaults(props));
        props.putIfAbsent(SOURCE, props.get("topics"));
    }

}
