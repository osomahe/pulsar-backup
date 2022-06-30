package net.osomahe.pulsarbackup.dump.control;

import io.quarkus.runtime.Quarkus;
import net.osomahe.pulsarbackup.dump.boundary.DumpFacade;
import net.osomahe.pulsarbackup.pulsar.boundary.PulsarFacade;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.Collections;


@CommandLine.Command(name = "dump", mixinStandardHelpOptions = true)
public class DumpCommand implements Runnable {

    @CommandLine.Option(names = {"-c", "--client"}, description = "Pulsar url e.g. pulsar://localhost:6650")
    String clientUrl;

    @CommandLine.Option(names = {"-a", "--admin"}, description = "Pulsar admin url e.g. http://localhost:8080")
    String adminUrl;

    @CommandLine.Option(names = {"-n", "--namespaces"}, description = "namespaces to backup e.g. tenant/namespace")
    String[] namespaces;

    @CommandLine.Option(names = {"-o", "--output"}, description = "Path to folder to dump data to e.g. /opt/pulsar-backup")
    String outputFolder;

    @Inject
    Logger log;

    @Inject
    DumpFacade facadeDump;

    @Inject
    PulsarFacade facadePulsar;

    @Override
    public void run() {
        try {
            facadeDump.dump(
                    facadePulsar.getPulsar(clientUrl, adminUrl),
                    getNamespaces(namespaces),
                    getFolder(outputFolder)
            );
        } catch (Exception e) {
            log.errorf(e, "Cannot dump pulsar data");
        }
    }


    private String[] getNamespaces(String[] cmdValue) {
        log.debugf("Pulsar namespaces via command line argument: %s", cmdValue);
        if (cmdValue != null) {
            return cmdValue;
        }
        var oNamespaces = ConfigProvider.getConfig().getOptionalValues("backup.namespaces", String.class);
        log.debugf("Pulsar namespaces via application.properties: %s", oNamespaces);
        if (oNamespaces.isEmpty()) {
            log.warnf("No namespace set. There is nothing to dump.");
            Quarkus.asyncExit(-1);
        }
        return oNamespaces.orElse(Collections.emptyList()).toArray(new String[0]);
    }

    private String getFolder(String cmdValue) {
        log.debugf("Output folder via command line argument: %s", cmdValue);
        if (cmdValue != null) {
            return cmdValue;
        }
        var oFolder = ConfigProvider.getConfig().getOptionalValue("backup.output", String.class);
        log.debugf("Backup folder via application.properties: %s", oFolder);
        if (oFolder.isEmpty()) {
            log.warnf("No folder set. There is nowhere to dump/restore.");
            Quarkus.asyncExit(-1);
        }
        return oFolder.orElse(null);
    }
}
