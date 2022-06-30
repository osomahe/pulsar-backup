package net.osomahe.pulsarbackup.pulsar.boundary;

import io.quarkus.runtime.ShutdownEvent;
import net.osomahe.pulsarbackup.pulsar.control.PulsarAdminService;
import net.osomahe.pulsarbackup.pulsar.control.PulsarClientService;
import net.osomahe.pulsarbackup.pulsar.entity.Pulsar;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.Set;
import java.util.TreeSet;


@ApplicationScoped
public class PulsarFacade {

    private Set<Pulsar> pulsars = new TreeSet<>();

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
            pulsar.client().close();
            pulsar.admin().close();
        }
    }
}
