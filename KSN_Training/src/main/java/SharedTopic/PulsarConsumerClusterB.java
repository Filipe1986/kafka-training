package sn_training.SharedTopic;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

/**
 * mvn compile exec:java@sharedpulsarconsumerclusterb
 **/
public class PulsarConsumerClusterB {

    public static void main(String[] args) throws Exception
    {
        String issuerUrl = "https://auth.streamnative.cloud/";
        String credentialsUrl = "file:///config/workspace/KSN-Training/src/main/resources/o-qmcug-kafkastudent2.json";
        String audience = "urn:sn:pulsar:o-qmcug:train2";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("<NEED NEW SERVICE URL>")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(issuerUrl),
                                new URL(credentialsUrl),
                                audience))
                .build();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("persistent://kafkastudent2/georep/sharedtopic")
                .subscriptionName("pulsarconsumerclusterB")
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