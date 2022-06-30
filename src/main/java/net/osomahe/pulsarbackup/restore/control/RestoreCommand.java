package net.osomahe.pulsarbackup.restore.control;

import io.quarkus.runtime.Quarkus;
import net.osomahe.pulsarbackup.pulsar.boundary.PulsarFacade;
import net.osomahe.pulsarbackup.restore.boundary.RestoreFacade;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import picocli.CommandLine;

import javax.inject.Inject;


@CommandLine.Command(name = "restore", mixinStandardHelpOptions = true)
public class RestoreCommand implements Runnable {


    @CommandLine.Option(names = {"-c", "--client"}, description = "Pulsar url e.g. pulsar://localhost:6650")
    String clientUrl;

    @CommandLine.Option(names = {"-a", "--admin"}, description = "Pulsar admin url e.g. http://localhost:8080")
    String adminUrl;

    @CommandLine.Option(names = {"-i", "--input"}, description = "Path to folder to restore data from e.g. /opt/pulsar-backup")
    String inputFolder;

    @CommandLine.Option(names = {"-f", "--force"}, description = "Write into topics even hen they already exist")
    Boolean force;

    @Inject
    Logger log;

    @Inject
    RestoreFacade facadeRestore;

    @Inject
    PulsarFacade facadePulsar;


    @Override
    public void run() {
        try {
            facadeRestore.restore(
                    facadePulsar.getPulsar(clientUrl, adminUrl),
                    getFolder(inputFolder),
                    getForce(force)
            );
        } catch (Exception e) {
            log.errorf(e, "Cannot restore pulsar data");
        }
    }

    private String getFolder(String cmdValue) {
        log.debugf("Input folder via command line argument: %s", cmdValue);
        if (cmdValue != null) {
            return cmdValue;
        }
        var oFolder = ConfigProvider.getConfig().getOptionalValue("backup.input", String.class);
        log.debugf("Backup folder via application.properties: %s", oFolder);
        if (oFolder.isEmpty()) {
            log.warnf("No folder set. There is nowhere to restore.");
            Quarkus.asyncExit(-1);
        }
        return oFolder.orElse(null);
    }

    private boolean getForce(Boolean cmdValue) {
        log.debugf("Forcing write via command line argument: %s", cmdValue);
        if (cmdValue != null) {
            return cmdValue;
        }
        var force = ConfigProvider.getConfig().getValue("backup.force", Boolean.class);
        log.debugf("Force write messages via application.properties: %s", force);
        return force;
    }
}
