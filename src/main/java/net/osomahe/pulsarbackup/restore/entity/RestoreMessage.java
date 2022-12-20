package net.osomahe.pulsarbackup.restore.entity;

import java.util.Base64;


public record RestoreMessage(byte[] key, Long eventTime, Long sequenceId, String value) {

    public static RestoreMessage fromLine(String line) {
        var parts = line.split("\\|");
        var decoder = Base64.getDecoder();

        var parsedKey = parts[0];
        var parsedEventTime = Long.parseLong(parts[1]);
        var parsedSequenceId = Long.parseLong(parts[2]);
        var parsedValue = parts[3];

        return new RestoreMessage(
                parsedKey != null && parsedKey.length() > 0 ? decoder.decode(parts[0]) : null,
                parsedEventTime > 0 ? parsedEventTime : null,
                parsedSequenceId > 0 ? parsedSequenceId : null,
                parsedValue
        );
    }
}
