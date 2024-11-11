package sn_training.KStreams;

import io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * mvn compile exec:java@kstreams
 **/
public class SNWordCountApplication {

    public static void main(final String[] args) {

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        // replace these config with your cluster
        String serverUrl = "pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:9093";
        String keyPath = "/config/workspace/KSN-Training/src/main/resources/o-qmcug-kafkastudent2.json";
        String audience = "urn:sn:pulsar:o-qmcug:train";

        // 1. Create Kafka Stream properties
        Properties props = new Properties();
        // stream application name
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkastudent2-wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // OAuth config
        props.setProperty("sasl.login.callback.handler.class", OauthLoginCallbackHandler.class.getName());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.setProperty("sasl.mechanism", "OAUTHBEARER");
        final String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"
                + " oauth.issuer.url=\"%s\""
                + " oauth.credentials.url=\"%s\""
                + " oauth.audience=\"%s\";";
        props.setProperty("sasl.jaas.config", String.format(jaasTemplate,
                "https://auth.streamnative.cloud/",
                "file://" + keyPath,
                audience
        ));

        // 2. Stream process
        String inputTopic = "kafkastudent2/georep/sharedtopic";

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(inputTopic);
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> {
                    System.out.println("stream application receive: " + textLine);
                    return Arrays.asList(textLine.toLowerCase().split("\\W+"));
                })
                .groupBy((key, word) -> word)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("kafkastudent2-counts-store"));
        wordCounts.toStream()
                .foreach((word, count) -> System.out.println("word: " + word + " -> " + count));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("stream") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            // 3. Start the stream
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}