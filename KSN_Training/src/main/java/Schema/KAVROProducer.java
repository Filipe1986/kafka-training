package sn_training.Schema;

import org.apache.kafka.clients.producer.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import java.util.concurrent.Future;

/**
 * mvn compile exec:java@schemaproducer
 **/
public class KAVROProducer {
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

                final Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord<>(props.get("topic1").toString(), avroRecord.get("item").toString(),avroRecord));
                final RecordMetadata recordMetadata = recordMetadataFuture.get();
                System.out.println("Sent record " + avroRecord.get("item") + " " + avroRecord.get("cost") + " with key " + avroRecord.get("item").toString());
            }
            producer.close();

        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}