package net.osomahe.pulsarbackup.dump.boundary;

import org.apache.pulsar.client.admin.PulsarAdmin;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;


@ApplicationScoped
public class DumpFacade {

    @Inject
    Logger log;

    @Inject
    PulsarClient pulsarClient;

    @Inject
    PulsarAdmin pulsarAdmin;

    @ConfigProperty(name = "pulsar.client-name")
    String readerName;

    public void dump(String pulsarUrl, String adminUrl, String[] namespaces, String outputFolder) throws Exception {
        if (outputFolder == null) {
            return;
        }
        var path = Paths.get(outputFolder);
        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }

        for (var namespace : namespaces) {
            dumpNamespace(pulsarUrl, adminUrl, namespace, outputFolder);
        }

    }

    private void dumpNamespace(String pulsarUrl, String adminUrl, String namespace, String parentFolder) throws IOException, PulsarAdminException {
        var path = Paths.get(parentFolder, namespace);
        if (Files.exists(path)) {
            throw new IllegalStateException("Cannot dump namespace[%s] data. Folder already exists!".formatted(namespace));
        }
        Files.createDirectories(path);

        var topics = pulsarAdmin.topics().getList(namespace);
        for (var topic : topics) {
            var messages = readTopic(topic);
            var fileName = topic.substring(topic.lastIndexOf("/") + 1);
            Files.write(Paths.get(path.toString(), fileName), messages);
        }
    }


    private List<String> readTopic(String topicName) throws IOException {
        try (Reader<byte[]> reader = pulsarClient.newReader()
                .readerName(readerName)
                .topic(topicName)
                .startMessageId(MessageId.earliest)
                .create()) {
            List<String> messages = new ArrayList<>();
            var encoder = Base64.getEncoder();
            while (reader.hasMessageAvailable()) {
                var message = reader.readNext(1, TimeUnit.SECONDS);
                if (message == null) {
                    break;
                }
                var key = message.getKeyBytes();
                var encodedKey = key != null ? encoder.encodeToString(key) : "";
                var value = message.getValue();
                messages.add("%s|%s".formatted(encodedKey, encoder.encodeToString(value)));
            }
            return messages;
        }
    }

}
