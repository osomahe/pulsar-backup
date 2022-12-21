# Pulsar Backup

This project backups Apache Pulsar instance. It has two modes **dump** or **restore**.

## Dump

This mode dumps data of selected namespaces into a folder. Dump creates folder for every tenant and namespace and in namespace folder it creates file for every topic. One line in topic backup file is single message. Backup contains `key`, `event time`, `sequence ID`, `value` of a message. Those are delimited by `|`.

Parameters:

* PULSAR_CLIENT_URL - url for Apache Pulsar connection default `pulsar://localhost:6650`
* PULSAR_ADMIN_URL - admin url to Apache Pulsar default `http://localhost:8080`
* BACKUP_OUTPUT - path to an empty folder to dump data from pulsar
* BACKUP_NAMESPACES - comma separated list of namespace to be dumped e.g. `default/customer,default/catalog`
* BACKUP_FORCE - replace files in output folder default false

### Run command

`docker run --name pulsar-backup --rm -e JAVA_ARGS=dump -e PULSAR_CLIENT_URL=pulsar://pulsar-broker:6650 -e PULSAR_ADMIN_URL=http://pulsar-broker:8080 -e BACKUP_OUTPUT=/pulsar-backup -e BACKUP_NAMESPACES=default/customer,default/catalog ghcr.io/osomahe/pulsar-backup:0.2.0`

## Restore

This mode restores dump data back into Apache Pulsar instance.

Parameters:

* PULSAR_CLIENT_URL - url for Apache Pulsar connection default `pulsar://localhost:6650`
* PULSAR_ADMIN_URL - admin url to Apache Pulsar default `http://localhost:8080`
* BACKUP_INPUT - path to a folder with dumped data
* BACKUP_FORCE - insert data into topic even if there is already some data default false

### Run command

`docker run --name pulsar-backup --rm -e JAVA_ARGS=restore -e PULSAR_CLIENT_URL=pulsar://pulsar-broker:6650 -e PULSAR_ADMIN_URL=http://pulsar-broker:8080 -e BACKUP_INPUT=/pulsar-backup ghcr.io/osomahe/pulsar-backup:0.2.0`
