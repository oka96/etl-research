FROM eclipse-temurin:8-jre

ARG CONNECT_DIST=rocketmq-connect-src/distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT

COPY ${CONNECT_DIST} /opt/rocketmq-connect
COPY rocketmq-connect-src/rocketmq-connect-sample/target/rocketmq-connect-sample-0.0.1-SNAPSHOT.jar /opt/rocketmq-connect/lib/rocketmq-connect-sample-0.0.1-SNAPSHOT.jar

ENV CONNECT_HOME=/opt/rocketmq-connect
WORKDIR /opt/rocketmq-connect

RUN mkdir -p /data/source /data/sink /tmp/rocketmq-connect-store

EXPOSE 8082

ENTRYPOINT ["java", "-Xms256m", "-Xmx512m", "-Dconnect.home.dir=/opt/rocketmq-connect", "-cp", "/opt/rocketmq-connect/conf:/opt/rocketmq-connect/lib/*", "org.apache.rocketmq.connect.runtime.StandaloneConnectStartup", "-c", "/etc/rocketmq-connect/connect-standalone.conf"]
