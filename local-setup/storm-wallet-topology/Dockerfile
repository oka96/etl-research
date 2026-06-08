FROM eclipse-temurin:17-jre

WORKDIR /app
COPY target/storm-wallet-topology-0.1.0.jar /app/storm-wallet-topology.jar

ENTRYPOINT ["java", "-jar", "/app/storm-wallet-topology.jar"]
