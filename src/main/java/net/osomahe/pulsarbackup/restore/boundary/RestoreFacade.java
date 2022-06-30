package net.osomahe.pulsarbackup.restore.boundary;

import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Paths;


@ApplicationScoped
public class RestoreFacade {

    @Inject
    Logger log;

    public void restore(String pulsarUrl, String adminUrl, String inputFolder, boolean force){
        if (inputFolder == null) {
            return;
        }
        var path = Paths.get(inputFolder);
        if (!Files.exists(path)) {
            log.warnf("Input folder does not exists. %s", inputFolder);
            return;
        }



    }


}
