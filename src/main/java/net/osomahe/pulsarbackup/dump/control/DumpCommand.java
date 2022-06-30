package net.osomahe.pulsarbackup.dump.control;

import io.quarkus.runtime.Quarkus;
import net.osomahe.pulsarbackup.dump.boundary.DumpFacade;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.Collections;

@CommandLine.Command(name = "dump", mixinStandardHelpOptions = true)
public class DumpCommand implements Runnable {

    @CommandLine.Option(names = {"-p", "--pulsar"}, description = "Pulsar url e.g. pulsar://localhost:6650")
    String pulsarUrl;

    @CommandLine.Option(names = {"-a", "--admin"}, description = "Pulsar admin url e.g. http://localhost:8080")
    String adminUrl;

    @CommandLine.Option(names = {"-n", "--namespaces"}, description = "namespaces to backup e.g. tenant/namespace")
    String[] namespaces;

    @CommandLine.Option(names = {"-o", "--output"}, description = "Path to folder to dump data to e.g. /opt/pulsar-backup")
    String outputFolder;

    @Inject
    Logger log;

    @Inject
    DumpFacade facade;

    @Override
    public void run() {
        try {
            facade.dump(
                    getPulsarUrl(pulsarUrl),
                    getAdminUrl(adminUrl),
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

    private String getPulsarUrl(String cmdValue) {
        log.debugf("Pulsar url via command line argument: %s", cmdValue);
        if (cmdValue != null) {
            return cmdValue;
        }
        var url = ConfigProvider.getConfig().getValue("pulsar.service-url", String.class);
        log.debugf("Pulsar url via application.properties: %s", url);
        return url;
    }

    private String getAdminUrl(String cmdValue) {
        log.debugf("Pulsar admin url via command line argument: %s", cmdValue);
        if (cmdValue != null) {
            return cmdValue;
        }
        var url = ConfigProvider.getConfig().getValue("pulsar.admin.url", String.class);
        log.debugf("Pulsar admin url via application.properties: %s", url);
        return url;
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
