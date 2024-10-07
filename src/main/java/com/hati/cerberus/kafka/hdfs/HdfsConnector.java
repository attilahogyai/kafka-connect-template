package com.hati.cerberus.kafka.hdfs;

import io.confluent.connect.hdfs.HdfsSinkConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

public class HdfsConnector extends HdfsSinkConnector {
    public Class<? extends Task> taskClass() {
        return HdfsWriterTask.class;
    }


    public ConfigDef config() {
        return HdfsWriterConfig.getConfig();
    }
}
