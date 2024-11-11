package sn_training.Transactions;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Future;

/**
 * mvn compile exec:java@transactionsproducer
 **/
public class KProducerTransactions {
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

        
        final Properties props = loadConfig("src/main/resources/client.properties.transactions");
        props.put("transactional.id", "my-transaction-id");
        props.put("enable.idempotence", "true");
        Producer<String, String> producer = new KafkaProducer<>(props);
        

        try {
            
            producer.initTransactions();
            producer.beginTransaction();

            String myMessage = "published by Kafka Producer";

            final Future<RecordMetadata> recordMetadataFutureA = producer.send(new ProducerRecord<>(props.get("topic1").toString(), myMessage));
            final Future<RecordMetadata> recordMetadataFutureB = producer.send(new ProducerRecord<>(props.get("topic2").toString(), myMessage));

            final RecordMetadata recordMetadataA = recordMetadataFutureA.get();
            final RecordMetadata recordMetadataB = recordMetadataFutureB.get();

            System.out.println("Sent record " + myMessage + " to two topics");
            producer.commitTransaction();
            

        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // Handle exceptions
            producer.close();
        } catch (KafkaException e) {
            // Handle other exceptions
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}