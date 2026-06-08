FROM eclipse-temurin:8-jre

WORKDIR /app
COPY target/rocketmq-streams-wallet-poc-0.1.0.jar /app/wallet-streams.jar

ENTRYPOINT ["java", "-jar", "/app/wallet-streams.jar"]
