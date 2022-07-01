package net.osomahe.pulsarbackup.pulsar.boundary;

import io.quarkus.runtime.ShutdownEvent;
import net.osomahe.pulsarbackup.pulsar.control.PulsarAdminService;
import net.osomahe.pulsarbackup.pulsar.control.PulsarClientService;
import net.osomahe.pulsarbackup.pulsar.entity.Pulsar;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;


@ApplicationScoped
public class PulsarFacade {

    private Set<Pulsar> pulsars = new HashSet<>();

    @Inject
    PulsarClientService serviceClient;

    @Inject
    PulsarAdminService serviceAdmin;


    public Pulsar getPulsar(String cmdLineArgPulsarUrl, String cmdLineArgAdminUrl) throws PulsarClientException {
        var pulsar = new Pulsar(
                serviceClient.createPulsarClient(cmdLineArgPulsarUrl),
                serviceAdmin.createPulsarAdmin((cmdLineArgAdminUrl))
        );

        pulsars.add(pulsar);
        return pulsar;
    }


    void shutdown(@Observes ShutdownEvent event) throws PulsarClientException {
        for (var pulsar : pulsars) {
            if (pulsar.client() != null) {
                pulsar.client().close();
            }
            if (pulsar.admin() != null) {
                pulsar.admin().close();
            }
        }
    }
}
