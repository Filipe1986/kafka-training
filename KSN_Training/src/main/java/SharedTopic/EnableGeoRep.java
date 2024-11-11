package sn_training.SharedTopic;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.URL;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.pulsar.client.admin.*;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * mvn compile exec:java@enablegeorep
 **/
public class EnableGeoRep {

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
                    .allowTlsInsecureConnection(false)
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
        String namespace = "kafkastudent2/georep";
        Set<String> clusters = new HashSet<String>(2);
        clusters.add("train");
        clusters.add("train2");

        //edit clusters list to enable geo-replication for the geo-rep namespace
        try {
            pulsarAdmin.namespaces().setNamespaceReplicationClusters(namespace,clusters);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }

        pulsarAdmin.close();
        System.out.println("Exiting");
    }
}