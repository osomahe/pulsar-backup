package net.osomahe.pulsarbackup.restore.entity;

import java.util.Base64;


public record RestoreMessage(byte[] key, long eventTime, long sequenceId, byte[] value) {

    public static RestoreMessage fromLine(String line) {
        var parts = line.split("\\|");
        var decoder = Base64.getDecoder();
        return new RestoreMessage(
                decoder.decode(parts[0]),
                Long.parseLong(parts[1]),
                Long.parseLong(parts[2]),
                decoder.decode(parts[3])
        );
    }
}
