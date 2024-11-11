package sn_training.Schema;

import org.apache.kafka.clients.consumer.*;
import java.io.*;
import java.nio.file.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;

/**
 * mvn compile exec:java@schemaconsumer
 **/
public class KAVROConsumer {
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public static void main(String[] args) throws Exception {
        try {
            final Properties props = loadConfig("src/main/resources/client.properties.schema");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(props.get("topic1").toString()));
            System.out.println("entering while loop");
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.println(record.value().get("item") + " " + record.value().get("cost"));
                }
            }

        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}