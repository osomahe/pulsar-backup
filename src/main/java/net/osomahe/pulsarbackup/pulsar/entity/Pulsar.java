package net.osomahe.pulsarbackup.pulsar.entity;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;


public record Pulsar(PulsarClient client, PulsarAdmin admin) {

}
