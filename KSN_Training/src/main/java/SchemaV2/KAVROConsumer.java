package sn_training.SchemaV2;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.streamnative.pulsar.handlers.kop.security.oauth.schema.OauthCredentialProvider;

/**
 * mvn compile exec:java@schemav2consumer
 **/
public class KAVROConsumer {

    public static void main(String[] args) throws Exception {

        //cluster configurations
        final String serverUrl = "pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:9093";
        String schemaRegistryUrl = "https://pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud/kafka";
        String keyPath = "/config/workspace/KSN-Training/src/main/resources/o-qmcug-kafkastudent2.json";
        String audience = "urn:sn:pulsar:o-qmcug:train";

        final String topic1 = "kafkastudent2-schemainput";
        final String tenant = "public"; //for schema, keep as public even if using multitenancy
        final String token = "<PASTE TOKEN HERE>";

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.setProperty("sasl.login.callback.handler.class", OauthLoginCallbackHandler.class.getName());
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "OAUTHBEARER");
        final String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"
                + " oauth.issuer.url=\"%s\""
                + " oauth.credentials.url=\"%s\""
                + " tenant=\"%s\""
                + " oauth.audience=\"%s\";";
        props.setProperty("sasl.jaas.config", String.format(jaasTemplate,
                "https://auth.streamnative.cloud/",
                "file://" + keyPath,
                tenant, //need tenant for schema, keep as public even if using multi-tenancy
                audience
        ));

        // Set configurations for schema registry using oauth along with producer/consumer oauth
        /*
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.BEARER_AUTH_CUSTOM_PROVIDER_CLASS, OauthCredentialProvider.class.getName());
        props.put(KafkaAvroSerializerConfig.BEARER_AUTH_CREDENTIALS_SOURCE, "CUSTOM");
        */
        
        // Set configuration for schema registry for token
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        props.put(KafkaAvroSerializerConfig.USER_INFO_CONFIG, String.format("%s:%s", "public", token)); // keep as public even if using multi-tenancy

        try {

            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singleton(topic1));
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