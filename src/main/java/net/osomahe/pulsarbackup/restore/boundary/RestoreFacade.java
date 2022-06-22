package net.osomahe.pulsarbackup.restore.boundary;

import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;


@ApplicationScoped
public class RestoreFacade {

    @Inject
    Logger log;

    public void restore(){
        log.infof("RESTORE");
    }


}
