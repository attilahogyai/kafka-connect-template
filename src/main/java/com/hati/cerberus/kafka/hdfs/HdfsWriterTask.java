package com.hati.cerberus.kafka.hdfs;

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
import java.util.Map;

public class HdfsWriterTask extends HdfsSinkTask {
    private static final Logger log = LoggerFactory.getLogger(HdfsWriterTask.class);
    private static final String OTHER_APP = "other";
    private KafkaProducer<String, Object> appProducer;
    private KafkaProducer<String, Object> userProducer;
    private String source;

    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        HdfsWriterConfig config = new HdfsWriterConfig(props);
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        appProducer = new KafkaProducer<>(producerProps);
        userProducer = new KafkaProducer<>(producerProps);
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

