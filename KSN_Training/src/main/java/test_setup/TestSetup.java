package sn_training.test_setup;

import java.net.URL;
import java.net.MalformedURLException;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;

/**
 * mvn compile exec:java@test_setup
 * 
 * synchronous client, and producer
 * 
 * 1. connects to streamnative cloud
 * 2. creates pulsar client synchronously
 * 3. create pulsar producer synchronously
 * 4. send one message
 * 5. close producer
 * 6. close client
 **/
public class TestSetup {

    // the URL to use for local standalone Pulsar
    public static String PULSAR_URL = "pulsar+ssl://pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:6651";

    // Security credentials
    private static String ISSUER_URL = "https://auth.streamnative.cloud";
    private static String AUDIENCE = "urn:sn:pulsar:o-qmcug:train";
    private static String CREDENTIALS_URL = "file:///config/workspace/KSN-Training/src/main/resources/o-qmcug-kafkastudent2.json";

    public static PulsarClient createClient() {
        PulsarClient pulsarClient = null;
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(PULSAR_URL)
                    .authentication(getAuth())
                    .allowTlsInsecureConnection(false)
                    .enableTlsHostnameVerification(true)
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return pulsarClient;
    }

    private static Authentication getAuth() {
        try {
            return AuthenticationFactoryOAuth2.clientCredentials(
                        new URL(ISSUER_URL), 
                        new URL(CREDENTIALS_URL), 
                        AUDIENCE);
        } catch (MalformedURLException e) {
            return null;
        }
    }

    public static void main(String[] args) {

        PulsarClient client = createClient();

        Producer<String> producer = null;
        String topic = "test_setup";

        try {
            producer = client.newProducer(Schema.STRING).topic(topic).create();
            System.out.println("producer created");
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        String message = "Test message";
        try {
            producer.newMessage().value(message).send();
            System.out.println("Published message " + message + " synchronously to topic " + topic);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        try {
            producer.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        System.out.println("Client closed: " + client.isClosed());
        try {
            client.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        System.out.println("Client closed: " + client.isClosed());

        System.out.println("Exiting");
    }
}