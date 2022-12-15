package net.osomahe.pulsarbackup.restore.entity;

import net.osomahe.pulsarbackup.pulsar.entity.PulsarSchema;

import java.io.Serializable;
import java.util.Base64;


public record RestoreMessage<T extends Serializable>(byte[] key, Long eventTime, Long sequenceId, T value) {

    public static RestoreMessage<? extends Serializable> fromLine(String line, PulsarSchema schemaDump) {
        var parts = line.split("\\|");
        var decoder = Base64.getDecoder();

        var parsedKey = parts[0];
        var parsedEventTime = Long.parseLong(parts[1]);
        var parsedSequenceId = Long.parseLong(parts[2]);
        var parsedValue = parts[3];

        var value = switch (schemaDump) {
            case STRING -> new String(decoder.decode(parsedValue));
            case BYTES -> decoder.decode(parsedValue);
        };

        return new RestoreMessage<>(
                parsedKey != null && parsedKey.length() > 0 ? decoder.decode(parts[0]) : null,
                parsedEventTime > 0 ? parsedEventTime : null,
                parsedSequenceId > 0 ? parsedSequenceId : null,
                value
        );
    }
}
