package net.osomahe.pulsarbackup.restore.boundary;

import net.osomahe.pulsarbackup.pulsar.entity.Pulsar;
import net.osomahe.pulsarbackup.pulsar.entity.PulsarSchema;
import net.osomahe.pulsarbackup.restore.entity.RestoreMessage;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


@ApplicationScoped
public class RestoreFacade {

    private static final Pattern TOPIC_PARTITION_PATTERN = Pattern.compile("\\-partition-\\d+");

    @ConfigProperty(name = "pulsar.client-name")
    String clientName;

    @ConfigProperty(name = "backup.strip-partitions")
    Boolean stripPartitions;

    @Inject
    Logger log;

    public void restore(Pulsar pulsar, String inputFolder, boolean force, PulsarSchema schemaDump, PulsarSchema schemaRestore) throws PulsarAdminException, IOException {
        if (inputFolder == null) {
            return;
        }
        var path = Paths.get(inputFolder);
        if (!Files.exists(path)) {
            log.warnf("Input folder does not exists. %s", inputFolder);
            return;
        }

        for (var f : path.toFile().listFiles()) {
            if (f.isDirectory()) {
                restoreTenant(f, force, pulsar, schemaDump, schemaRestore);
            } else {
                log.infof("Skipping tenant file %s because it is not a folder", f.getName());
            }
        }
    }

    private void restoreTenant(File folderTenant, boolean force, Pulsar pulsar, PulsarSchema schemaDump, PulsarSchema schemaRestore) throws PulsarAdminException, IOException {
        log.infof("Restoring tenant %s", folderTenant.getName());
        for (var f : folderTenant.listFiles()) {
            if (f.isDirectory()) {
                restoreNamespace(f, force, pulsar, schemaDump, schemaRestore);
            } else {
                log.infof("Skipping namespace file %s because it is not a folder", f.getName());
            }
        }
    }

    private void restoreNamespace(File folderNamespace, boolean force, Pulsar pulsar, PulsarSchema schemaDump, PulsarSchema schemaRestore) throws PulsarAdminException, IOException {
        log.infof("Restoring namespace %s", folderNamespace.getName());
        for (var f : folderNamespace.listFiles()) {
            if (f.isFile() && !f.isHidden()) {
                restoreTopic(f, force, pulsar, schemaDump, schemaRestore);
            } else {
                log.infof("Skipping topic file %s because it is not a valid file", f.getName());
            }
        }
    }

    private void restoreTopic(File fileTopic, boolean force, Pulsar pulsar, PulsarSchema schemaDump, PulsarSchema schemaRestore) throws PulsarAdminException, IOException {
        log.infof("Restoring from file %s", fileTopic.getAbsolutePath());
        var topicName = fileTopic.getName();
        var namespace = fileTopic.getParentFile().getName();
        var tenant = fileTopic.getParentFile().getParentFile().getName();
        var topicNameFull = "persistent://%s/%s/%s".formatted(tenant, namespace, topicName);
        if (stripPartitions) {
            // strip partition signature
            topicNameFull = TOPIC_PARTITION_PATTERN.matcher(topicNameFull).replaceAll("");
        }
        log.infof("Restoring topic %s", topicNameFull);

        var numberOfEntries = getNumberOfEntries(topicNameFull, namespace, tenant, pulsar.admin());
        if (numberOfEntries > 0 && !force) {
            log.warnf("Cannot restore topic %s because it contains %s entries", topicNameFull, numberOfEntries);
            return;
        }
        var schemaProducer = schemaRestore == null ? Schema.BYTES : schemaRestore.getSchema();

        var messages = Files.readAllLines(fileTopic.toPath()).stream().map(line -> RestoreMessage.fromLine(line, schemaDump)).toList();

        try (var producer = pulsar.client().newProducer(schemaProducer)
                .compressionType(CompressionType.LZ4)
                .hashingScheme(HashingScheme.Murmur3_32Hash)
                .sendTimeout(10, TimeUnit.SECONDS)
                .producerName(clientName)
                .topic(topicNameFull)
                .create()) {

            for (var message : messages) {
                var builder = producer.newMessage();
                if (message.key() != null) {
                    builder = builder.keyBytes(message.key());
                }
                if (message.eventTime() != null) {
                    builder = builder.eventTime(message.eventTime());
                }
                if (message.sequenceId() != null) {
                    builder = builder.sequenceId(message.sequenceId());
                }
                builder.value(message.value()).send();
            }
        }
    }

    private long getNumberOfEntries(String topicNameFull, String namespace, String tenant, PulsarAdmin admin) throws PulsarAdminException {
        if (admin.tenants().getTenants().contains(tenant)) {
            var namespaceFull = "%s/%s".formatted(tenant, namespace);
            if (admin.namespaces().getNamespaces(tenant).contains(namespaceFull)) {
                if (admin.topics().getList(namespaceFull).contains(topicNameFull)) {
                    return admin.topics().getInternalStats(topicNameFull).numberOfEntries;
                }
            }
        }
        return 0;
    }
}
