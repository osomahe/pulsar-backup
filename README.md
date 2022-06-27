```
QUARKUS_LOG_CATEGORY__NET_OSOMAHE__LEVEL=DEBUG BACKUP_NAMESPACES=eb/project-offset BACKUP_FOLDER=/tmp/pulsar-backup mvn clean quarkus:dev -Dquarkus.args="dump"  

mvn clean package -Dquarkus.package.type=uber-jar && java -jar target/*-runner.jar restore -p a -o b -n a,b

mvn clean quarkus:dev -Dquarkus.args="dump -p a -o b -n a,b"
```
