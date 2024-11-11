package sn_training.SharedTopic;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

/**
 * mvn compile exec:java@sharedpulsarconsumer
 **/
public class PulsarConsumer {

    public static void main(String[] args) throws Exception
    {
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

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("persistent://kafkastudent2/georep/sharedtopic")
                .subscriptionName("pulsarconsumerclusterA")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        for (int i = 0; i < 1000; i++) {
            Message<String> msg = consumer.receive();
            consumer.acknowledge(msg);
            System.out.println(msg.getValue() + " consumed by Pulsar Consumer");
        }

        consumer.close();
        client.close();

    }

}