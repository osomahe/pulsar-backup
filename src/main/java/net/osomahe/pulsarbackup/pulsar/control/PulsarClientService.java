package net.osomahe.pulsarbackup.pulsar.control;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Optional;


@ApplicationScoped
public class PulsarClientService {

    @ConfigProperty(name = "pulsar.client-url")
    Optional<String> oClientUrl;

    @Inject
    Logger log;

    public PulsarClient createPulsarClient(String clientUrl) throws PulsarClientException {
        if (clientUrl == null || clientUrl.isEmpty() && oClientUrl.isEmpty()) {
            log.warnf("Cannot init clientUrl! Command line argument is empty neither property pulsar.client-url is set.");
            return null;
        }

        if (clientUrl == null || clientUrl.isEmpty()) {
            clientUrl = oClientUrl.get();
        }
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(clientUrl).allowTlsInsecureConnection(true);

        return clientBuilder.build();
    }
}
