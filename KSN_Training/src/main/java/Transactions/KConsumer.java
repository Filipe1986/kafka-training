package sn_training.Transactions;

import org.apache.kafka.clients.consumer.*;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * mvn compile exec:java@transactionsconsumer
 **/
public class KConsumer {
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
            final Properties props = loadConfig("src/main/resources/client.properties.transactions");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-transactions");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put("isolation.level","read_committed");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(props.get("topic1").toString()));
            System.out.println("entering while loop");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.toString());
                }
            }

        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}