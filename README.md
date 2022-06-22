```
mvn clean package -Dquarkus.package.type=uber-jar && java -jar target/*-runner.jar restore -p a -o b -n a,b

mvn clean quarkus:dev -Dquarkus.args="dump -p a -o b -n a,b"
```