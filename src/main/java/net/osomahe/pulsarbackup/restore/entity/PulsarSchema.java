package net.osomahe.pulsarbackup.restore.entity;

public enum PulsarSchema {

    BYTES,

    STRING;

    public static PulsarSchema of(String name) {
        for (var schema : values()) {
            if (schema.name().equalsIgnoreCase(name)) {
                return schema;
            }
        }
        throw new IllegalArgumentException("Cannot create PulsarSchema for String value: %s".formatted(name));
    }
}
