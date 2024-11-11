package sn_training.SchemaV2;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import java.util.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import java.util.concurrent.Future;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler;
import io.streamnative.pulsar.handlers.kop.security.oauth.schema.OauthCredentialProvider;

/**
 * mvn compile exec:java@schemav2producer
 **/
public class KAVROProducer {

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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
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

            Producer<String, GenericRecord> producer = new KafkaProducer<>(props);

            String userSchema = "{\"type\":\"record\",\"name\":\"order\","
                    + "\"namespace\":\"org.apache.kafka\","
                    + "\"fields\":[{\"name\":\"item\",\"type\":\"string\"},{\"name\":\"cost\",\"type\":\"int\"}]}";
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(userSchema);

            for(int i = 0; i < 10; i++){
                GenericRecord avroRecord = new GenericData.Record(schema);
                if( i%2 == 0) {
                    avroRecord.put("item", "tv");
                } else {
                    avroRecord.put("item", "fridge");
                }
                avroRecord.put("cost", i);

                final Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord<>(topic1, avroRecord.get("item").toString(),avroRecord));
                final RecordMetadata recordMetadata = recordMetadataFuture.get();
                System.out.println("Sent record " + avroRecord.get("item") + " " + avroRecord.get("cost") + " with key " + avroRecord.get("item").toString());
            }
            producer.close();

        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}