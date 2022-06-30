package net.osomahe.pulsarbackup.pulsar.control;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Handler;
import java.util.logging.LogManager;


@ApplicationScoped
public class PulsarAdminService {

    @ConfigProperty(name = "pulsar.admin-url")
    Optional<String> oAdminUrl;

    @Inject
    Logger log;

    public PulsarAdmin createPulsarAdmin(String adminUrl) throws PulsarClientException {
        var savedHandlers = saveLogHandlers();

        if (adminUrl == null || adminUrl.isEmpty() && oAdminUrl.isEmpty()) {
            log.warnf("Cannot init adminUrl! Command line argument is empty neither property pulsar.admin-url is set.");
            return null;
        }

        if (adminUrl == null || adminUrl.isEmpty()) {
            adminUrl = oAdminUrl.get();
        }

        PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().serviceHttpUrl(adminUrl).allowTlsInsecureConnection(true);

        var pulsarAdmin = adminBuilder.build();

        restoreLogHandlers(savedHandlers);
        return pulsarAdmin;
    }

    /**
     * Handlers need to be saved because {@link org.apache.pulsar.client.admin.internal.PulsarAdminImpl}
     * removes them and creates bridge and that breaks all logging in quarkus.
     * Log handlers are restored by method restoreLogHandlers() after PulsarAdmin init is completed.
     *
     * @return
     */
    private List<Handler> saveLogHandlers() {
        java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
        var savedHandlers = new ArrayList<Handler>();
        for (var handler : rootLogger.getHandlers()) {
            savedHandlers.add(handler);
        }
        return savedHandlers;
    }

    /**
     * Check docs in method saveLogHandlers()
     *
     * @param savedHandlers
     */
    private void restoreLogHandlers(List<Handler> savedHandlers) {
        java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");

        for (var handler : rootLogger.getHandlers()) {
            rootLogger.removeHandler(handler);
        }
        for (var handler : savedHandlers) {
            rootLogger.addHandler(handler);
        }
    }
}
