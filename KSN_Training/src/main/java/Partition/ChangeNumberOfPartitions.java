package sn_training.Partition;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.URL;
import java.net.MalformedURLException;

import org.apache.pulsar.client.admin.*;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * mvn compile exec:java@changepartitions
 **/
public class ChangeNumberOfPartitions {

    public static String PULSAR_ADMIN_URL = "https://pc-182d65af.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud";

    // Security credentials
    private static String ISSUER_URL = "https://auth.streamnative.cloud/";
    private static String AUDIENCE = "urn:sn:pulsar:o-qmcug:train";
    private static String CREDENTIALS_URL = "file:///config/workspace/KSN-Training/src/main/resources/o-qmcug-kafkastudent2.json";

    public static PulsarAdmin createAdmin() {
        System.out.println("entering createAdmin");
        PulsarAdmin pulsarAdmin = null;
        try {
            pulsarAdmin = PulsarAdmin.builder()
                    .authentication(getAuth())
                    .serviceHttpUrl(PULSAR_ADMIN_URL)
                    //.allowTlsInsecureConnection(false)
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        System.out.println("returning pulsarAdmin");
        return pulsarAdmin;
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

        PulsarAdmin pulsarAdmin = createAdmin();
        String topic = "kafkastudent2-schemainput";
        Integer partitions = 5;

        //change the number of partitions of an existing topic
        try {
            pulsarAdmin.topics().updatePartitionedTopic(topic, partitions);
            System.out.println("changed number of partitions");
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }

        pulsarAdmin.close();
        System.out.println("Exiting");
    }
}