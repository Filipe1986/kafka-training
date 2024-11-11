package sn_training.SharedTopic;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

/**
 * mvn compile exec:java@sharedpulsarproducer
 **/
public class PulsarProducer {

    public static void main(String[] args) throws Exception
    {
        try {
            String issuerUrl = "https://auth.streamnative.cloud/";
            String credentialsUrl = "file:///config/workspace/KSN-Training/src/main/resources/o-qmcug-kafkastudent2.json";
            String audience = "urn:sn:pulsar:o-qmcug:train";

            PulsarClient client = PulsarClient.builder()
                    .serviceUrl("pulsar+ssl://pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:6651")
                    .authentication(
                            AuthenticationFactoryOAuth2.clientCredentials(new URL(issuerUrl),
                                    new URL(credentialsUrl),
                                    audience))
                    .build();

            Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic("persistent://kafkastudent2/georep/sharedtopic")
                    .create();

            String message = "published by Pulsar Producer";
            MessageId msgID = producer.send(message);
            System.out.println(message);

            producer.close();
            client.close();
        } catch (Exception e) {
            throw new Exception(e);
        }
    }
}