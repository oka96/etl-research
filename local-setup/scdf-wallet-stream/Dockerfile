FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM redpandadata/redpanda:v25.2.9 AS redpanda

FROM eclipse-temurin:21-jre

WORKDIR /app
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=redpanda /usr/bin/rpk /usr/bin/rpk
COPY --from=redpanda /opt/redpanda /opt/redpanda
COPY --from=build /workspace/target/scdf-wallet-stream-0.1.0.jar /app/app.jar

EXPOSE 18086
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
