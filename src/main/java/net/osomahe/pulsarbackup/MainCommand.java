package net.osomahe.pulsarbackup;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import net.osomahe.pulsarbackup.dump.boundary.DumpFacade;
import net.osomahe.pulsarbackup.restore.boundary.RestoreFacade;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import javax.inject.Inject;


@TopCommand
@Command(mixinStandardHelpOptions = true, subcommands = {DumpCommand.class, RestoreCommand.class})
public class MainCommand {

}

@Command(name = "dump", mixinStandardHelpOptions = true)
class DumpCommand implements Runnable {

    @CommandLine.Option(names = {"-p", "--pulsar"}, description = "Pulsar url e.g. pulsar://localhost:6650", required = true)
    String pulsarUrl;

    @CommandLine.Option(names = {"-o", "--output"}, description = "Path to output folder e.g. /opt/pulsar-dump", required = true)
    String outputFolder;

    @CommandLine.Option(names = {"-n", "--namespaces"}, description = "namespaces to backup e.g. tenant/namespace", required = true)
    String[] namespaces;

    @Inject
    DumpFacade facade;

    @Override
    public void run() {
        facade.dump();
    }
}

@Command(name = "restore", mixinStandardHelpOptions = true)
class RestoreCommand implements Runnable {

    @CommandLine.Option(names = {"-p", "--pulsar"}, description = "Pulsar url e.g. pulsar://localhost:6650", required = true)
    String pulsarUrl;

    @CommandLine.Option(names = {"-o", "--output"}, description = "Path to output folder e.g. /opt/pulsar-dump", required = true)
    String outputFolder;

    @CommandLine.Option(names = {"-n", "--namespaces"}, description = "namespaces to backup e.g. tenant/namespace", required = true)
    String[] namespaces;

    @Inject
    RestoreFacade facade;

    @Override
    public void run() {
        facade.restore();
    }
}
