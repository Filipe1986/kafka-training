package sn_training.SharedTopicV2;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * mvn compile exec:java@sharedv2kafkaconsumer
 **/
public class KConsumer {

    public static void main(String[] args) throws Exception {
        try {

            //cluster configurations
            final String serverUrl = "pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:9093";
            final String jwtToken = "<PASTE TOKEN HERE>";
            final String token = "token:" + jwtToken;

            final String topic1 = "kafkastudent2/georep/sharedtopic";
            final String username = "public/default"; //keep username as public/default

            final Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaconsumerclusterA");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            props.put("sasl.mechanism", "PLAIN");
            props.put("sasl.jaas.config", String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    username, token));

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic1));
            System.out.println("entering while loop");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value() + " consumed by Kafka Consumer");
                }
            }

        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}