package sn_training.SharedTopic;

import org.apache.kafka.clients.producer.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Future;

/**
 * mvn compile exec:java@sharedkafkaproducer
 **/
public class KProducer {
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

        final Properties props = loadConfig("src/main/resources/client.properties.sharedtopic");
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            String myMessage = "published by Kafka Producer";

            final Future<RecordMetadata> recordMetadataFutureA = producer.send(new ProducerRecord<>(props.get("topic1").toString(), myMessage));

            final RecordMetadata recordMetadataA = recordMetadataFutureA.get();

            System.out.println(myMessage);

            producer.close();

        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}