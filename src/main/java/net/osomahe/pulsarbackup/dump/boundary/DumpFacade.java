package net.osomahe.pulsarbackup.dump.boundary;

import net.osomahe.pulsarbackup.pulsar.entity.Pulsar;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;


@ApplicationScoped
public class DumpFacade {

    @Inject
    Logger log;

    @ConfigProperty(name = "pulsar.client-name")
    String clientName;

    public void dump(Pulsar pulsar, String[] namespaces, String outputFolder, boolean force) throws Exception {
        if (outputFolder == null) {
            return;
        }
        var path = Paths.get(outputFolder);
        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }

        for (var namespace : namespaces) {
            dumpNamespace(pulsar, namespace, outputFolder, force);
        }

    }

    private void dumpNamespace(Pulsar pulsar, String namespace, String parentFolder, boolean force) throws IOException, PulsarAdminException {
        log.infof("Dumping namespace %s", namespace);
        var path = Paths.get(parentFolder, namespace);
        Files.createDirectories(path);

        var topics = pulsar.admin().topics().getList(namespace);
        for (var topic : topics) {
            if (topic.startsWith("non-persistent")) {
                log.warnf("Will not dump non-persistent topic %s", topic);
                continue;
            }
            dumpTopicData(topic, pulsar.client(), path, force);
        }
    }


    private void dumpTopicData(String topicName, PulsarClient pulsarClient, Path folder, boolean force) throws IOException {
        log.infof("Dumping topic %s", topicName);
        var fileName = topicName.substring(topicName.lastIndexOf("/") + 1);
        var path = Paths.get(folder.toString(), fileName);
        if (Files.exists(path)) {
            if (force) {
                Files.delete(path);
            } else {
                throw new IllegalStateException("Cannot dump data file: %s already exists.".formatted(path.toString()));
            }
        }
        Files.createFile(path);
        try (Reader<byte[]> reader = pulsarClient.newReader()
                .readerName(clientName)
                .topic(topicName)
                .startMessageId(MessageId.earliest)
                .create()) {
            var encoder = Base64.getEncoder();
            int count = 0;
            while (reader.hasMessageAvailable()) {
                var message = reader.readNext(1, TimeUnit.SECONDS);
                if (message == null) {
                    break;
                }
                var key = message.getKeyBytes();
                var encodedKey = key != null ? encoder.encodeToString(key) : "";
                var value = message.getValue();
                Files.write(path,
                        Collections.singletonList("%s|%s|%s|%s".formatted(encodedKey, message.getEventTime(), message.getSequenceId(), encoder.encodeToString(value))),
                        StandardOpenOption.APPEND);
                count++;
            }
            log.infof("%s messages was dumped for topic %s", count, topicName);
        }
    }
}
