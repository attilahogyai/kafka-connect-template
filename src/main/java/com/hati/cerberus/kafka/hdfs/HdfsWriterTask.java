package com.hati.cerberus.kafka.hdfs;

import com.hati.util.Iso8601DateTime;
import io.confluent.connect.hdfs.HdfsSinkTask;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsWriterTask extends HdfsSinkTask {
    private static final Logger log = LoggerFactory.getLogger(HdfsWriterTask.class);
    private static final long SEND_INTERVAL_MS = 30000;
    private static final String OTHER_APP = "other";
    private KafkaProducer<String, Object> appProducer;
    private KafkaProducer<String, Object> userProducer;
    private String appTopic;
    private String userTopic;
    private long lastSendTime = System.currentTimeMillis();
    private HashMap<String, Long> appStat = new HashMap<>();
    private HashMap<String, List<String>> userStat = new HashMap<>();
    private String source;
    private int currentPartition = 0;
    private static final int NUM_OF_PARTITIONS = 16;

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        HdfsWriterConfig config = new HdfsWriterConfig(props);
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(HdfsWriterConfig.BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        appProducer = new KafkaProducer<>(producerProps);
        userProducer = new KafkaProducer<>(producerProps);
        appTopic = config.getString(HdfsWriterConfig.APP_TOPIC);
        userTopic = config.getString(HdfsWriterConfig.USER_TOPIC);
        source = config.getString(HdfsWriterConfig.SOURCE);

    }

    @Override
    public void stop() throws ConnectException {
        super.stop();
        if(appProducer!=null){
            appProducer.close();
        }
        if(userProducer!=null){
            userProducer.close();
        }
    }

    public void put(Collection<SinkRecord> records) throws ConnectException {
        long offset = 0;
        try {
            super.put(records);
            for (SinkRecord record : records) {
                Struct struct = (Struct) record.value();

                long date = struct.getInt64("log_date");
                String dateHour = Iso8601DateTime.formatEpochMilliToHour(date);
                String appName = struct.get("app_name") == null ? OTHER_APP : struct.getString("app_name");
                String key = dateHour + "@" + appName + "@" + source;

                appStat.merge(key, 1L, Long::sum);

            }

        } catch (ConnectException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error starting UserStatWriterTask: {}", e.getMessage());
            records.forEach(c -> {
                Struct v = (Struct) c.value();
                log.error("record: {}", v.toString());
            });
            throw e;
        }

    }



}

