package com.hati.cerberus.kafka.hdfs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hati.util.Iso8601DateTime;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.time.format.DateTimeParseException;
import java.util.Map;

public class LogConverter implements Converter {
    private static final Logger log = LoggerFactory.getLogger(LogConverter.class);
    public static final String LOG = "log";
    public static final String RAW = "raw";
    public static final String APP_NAME = "app_name";
    public static final String TIMESTAMP = "log_date";
    public static final String IP_ADDRESS = "ip";
    private static final String UNKNOWN = "unknown";
    private final ObjectMapper mapper = new ObjectMapper();

    protected long count = 0;
    protected long totalCount = 0;
    protected long lastDisplay = System.currentTimeMillis();

    protected int statPeriod = 30000;
    private final DecimalFormat lFormat = new DecimalFormat("###,###");

    private ConverterConfig config;
    private static Schema schema;

    {
        schema = SchemaBuilder.struct().name("com.brickztech.otp.LogSchema")
                .field(APP_NAME, Schema.STRING_SCHEMA)
                .field(TIMESTAMP, Schema.INT64_SCHEMA)
                .field(LOG, Schema.STRING_SCHEMA)
                .field(IP_ADDRESS, Schema.STRING_SCHEMA)

                .build();
    }

    @Override
    public ConfigDef config() {
        return ConverterConfig.CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.info("Configuring LogConverter");
        config = new ConverterConfig(configs);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            var log = (Map) value;
            return mapper.writeValueAsBytes(log);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected void stat() {
        count++;
        if (System.currentTimeMillis() - lastDisplay > statPeriod) {
            totalCount += count;
            log.info("{}-{} : overallBatchSize {} , nr of batches {} , totalCount {} , count {} for task {} , ~ entry/s {} ,offset {} ",
                    "LogConverter", "?", "?", "?", lFormat.format(totalCount), lFormat.format(count), "?", count / 30, "?");
            count = 0;
            lastDisplay = System.currentTimeMillis();
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            stat();
            String json = new String(value, StandardCharsets.UTF_8);
            Struct result = new Struct(schema.schema());

            var currentValue = mapper.readValue(json, Map.class);
            long dateField = 0L;
            try{
                String dateS = (String)currentValue.get(config.getDateField());
                dateField = Iso8601DateTime.parseToEpochMilli(dateS);
            }catch(DateTimeParseException e){
                log.error("log timestamp parse error {}, rawData: {}", e.getMessage(), json);
            }
            result.put(LOG, currentValue.getOrDefault(config.getMessage(), "N/A"));
            result.put(APP_NAME, currentValue.getOrDefault(config.getApplicationField(), UNKNOWN));
            result.put(TIMESTAMP, dateField);
            result.put(IP_ADDRESS, currentValue.getOrDefault(config.getSourceIP(), UNKNOWN));
            result.put(RAW, json);
            return new SchemaAndValue(schema, result);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
