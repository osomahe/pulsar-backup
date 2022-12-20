package net.osomahe.pulsarbackup;

import io.quarkus.picocli.runtime.PicocliCommandLineFactory;
import picocli.CommandLine;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;


@ApplicationScoped
public class CommandLineConfig {

    @Produces
    CommandLine customCommandLine(PicocliCommandLineFactory factory) {
        return factory.create().setCaseInsensitiveEnumValuesAllowed(true);
    }
}
