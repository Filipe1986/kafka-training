package sn_training.Partition;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.*;

/**
 * mvn compile exec:java@partitionkproducer
 **/
public class KProducer {

    public static void main(String[] args) throws Exception {

        //cluster configurations
        final String serverUrl = "pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:9093";
        final String jwtToken = "<PASTE TOKEN HERE>";
        final String token = "token:" + jwtToken;

        final String topic1 = "kafkastudent2-schemainput";
        final String username = "public/default"; // keep username as public/default

        //no edits needed below here
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                username, token));

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 10000; i++) {
                String myMessage = i + " published by Kafka Producer";
                //don't use get on the future to publish quickly
                producer.send(new ProducerRecord<>(topic1, myMessage));
                System.out.println(myMessage);
            }

        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            producer.close();
        }
    }
}