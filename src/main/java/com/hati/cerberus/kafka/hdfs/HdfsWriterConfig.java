package com.hati.cerberus.kafka.hdfs;


import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class HdfsWriterConfig extends HdfsSinkConnectorConfig {

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String USER_TOPIC = "user.topic";
    public static final String APP_TOPIC = "app.topic";
    public static final String SOURCE = "source";


    public static ConfigDef newConfigDef() {
        ConfigDef configDef = HdfsSinkConnectorConfig.newConfigDef();
        return extend(configDef);
    }

    private static ConfigDef extend(ConfigDef configDef) {
        configDef.define(BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "Bootstrap servers");
        configDef.define(USER_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "User Topic");
        configDef.define(APP_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "App Topic");
        configDef.define(SOURCE, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, "Source");
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

    protected HdfsWriterConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
    }
}
