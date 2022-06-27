package net.osomahe.pulsarbackup;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import net.osomahe.pulsarbackup.dump.control.DumpCommand;
import net.osomahe.pulsarbackup.restore.boundary.RestoreFacade;
import picocli.CommandLine.Command;

import javax.inject.Inject;


@TopCommand
@Command(mixinStandardHelpOptions = true, subcommands = {DumpCommand.class, RestoreCommand.class})
public class MainCommand {
}


@Command(name = "restore", mixinStandardHelpOptions = true)
class RestoreCommand implements Runnable {

    @Inject
    RestoreFacade facade;

    @Override
    public void run() {
        facade.restore();
    }
}
