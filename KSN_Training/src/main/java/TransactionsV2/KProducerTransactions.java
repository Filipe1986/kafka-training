package sn_training.TransactionsV2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.*;
import java.util.concurrent.Future;

/**
 * mvn compile exec:java@transactionsv2producer
 **/
public class KProducerTransactions {

    public static void main(String[] args) throws Exception {

        //cluster configurations
        final String serverUrl = "pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:9093";
        final String jwtToken = "<PASTE TOKEN HERE>";
        final String token = "token:" + jwtToken;

        final String topic1 = "kafkastudent2-transactions1";
        final String topic2 = "kafkastudent2-transactions2";
        final String username = "public/default"; //keep username as public/default

        //no edits needed below here
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put("transactional.id", "my-transaction-id");
        props.put("enable.idempotence", "true");

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                username, token));

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            producer.initTransactions();
            producer.beginTransaction();

            String myMessage = "published by Kafka Producer";

            final Future<RecordMetadata> recordMetadataFutureA = producer.send(new ProducerRecord<>(topic1, myMessage));
            final Future<RecordMetadata> recordMetadataFutureB = producer.send(new ProducerRecord<>(topic2, myMessage));

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