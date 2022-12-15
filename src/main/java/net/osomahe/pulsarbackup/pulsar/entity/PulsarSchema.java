package net.osomahe.pulsarbackup.pulsar.entity;

import org.apache.pulsar.client.api.Schema;

import java.io.Serializable;


public enum PulsarSchema {

    BYTES(Schema.BYTES),

    STRING(Schema.STRING);

    private final Schema<? extends Serializable> schema;

    PulsarSchema(Schema<? extends Serializable> schema) {
        this.schema = schema;
    }

    public Schema<? extends Serializable> getSchema() {
        return schema;
    }
}
