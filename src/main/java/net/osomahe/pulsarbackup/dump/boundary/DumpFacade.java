package net.osomahe.pulsarbackup.dump.boundary;

import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;


@ApplicationScoped
public class DumpFacade {

    @Inject
    Logger log;

    public void dump() {
        log.infof("DUMP");
    }

}
