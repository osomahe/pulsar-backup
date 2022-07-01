FROM maven:3.8-openjdk-17-slim as builder

RUN mkdir /app
COPY src /app/src
COPY pom.xml /app
WORKDIR /app
RUN mvn clean package -Dquarkus.package.type=uber-jar

FROM registry.access.redhat.com/ubi8/openjdk-17:1.11

ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en'

COPY --from=builder --chown=185 /app/target/*-runner.jar /deployments/quarkus-runner.jar

EXPOSE 8080
USER 185
ENV JAVA_OPTS="-Djava.util.logging.manager=org.jboss.logmanager.LogManager"
ENV JAVA_APP_JAR="/deployments/quarkus-runner.jar"