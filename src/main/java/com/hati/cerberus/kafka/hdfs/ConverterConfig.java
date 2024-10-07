package com.hati.cerberus.kafka.hdfs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ConverterConfig extends AbstractConfig {

    public static final String APPLICATION_FIELD_NAME = "application.field";
    public static final String DATE_FIELD_NAME = "date.field";

    public static final String SOURCE_IP = "ip";

    public static final String MESSAGE = "message";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(APPLICATION_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Application field name")
            .define(SOURCE_IP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Source IP")
            .define(MESSAGE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Log message")
            .define(DATE_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Date field name");


    public ConverterConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, originals, true);
    }

    public String getApplicationField() {
        return getString(APPLICATION_FIELD_NAME);
    }
    public String getSourceIP() {
        return getString(SOURCE_IP);
    }

    public String getDateField() {
        return getString(DATE_FIELD_NAME);
    }
    public String getMessage() {
        return getString(MESSAGE);
    }

}
