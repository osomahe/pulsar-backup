package net.osomahe.pulsarbackup;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import net.osomahe.pulsarbackup.dump.control.DumpCommand;
import net.osomahe.pulsarbackup.restore.control.RestoreCommand;
import picocli.CommandLine.Command;


@TopCommand
@Command(mixinStandardHelpOptions = true, subcommands = {DumpCommand.class, RestoreCommand.class})
public class MainCommand {
}
