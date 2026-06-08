FROM eclipse-temurin:8-jdk

WORKDIR /opt/rocketmq-eventbridge

COPY . /opt/rocketmq-eventbridge

RUN mkdir -p /tmp/eventbridge-classes/BOOT-INF/classes \
    && printf '%s\n' \
      'rocketmq.namesrvAddr=phase2-rocketmq-namesrv:9876' \
      'rocketmq.consumer.pullTimeOut=3000' \
      'rocketmq.consumer.pullBatchSize=32' \
      'rocketmq.accessChannel=LOCAL' \
      'rocketmq.namespace=' \
      'rocketmq.consumer.accessKey=' \
      'rocketmq.consumer.secretKey=' \
      'rocketmq.consumer.socks5UserName=' \
      'rocketmq.consumer.socks5Password=' \
      'rocketmq.consumer.socks5Endpoint=' \
      > /tmp/eventbridge-classes/BOOT-INF/classes/runtime.properties \
    && cd /tmp/eventbridge-classes \
    && jar uf /opt/rocketmq-eventbridge/rocketmq-eventbridge.jar BOOT-INF/classes/runtime.properties \
    && rm -rf /tmp/eventbridge-classes \
    && mkdir -p /data/sink /tmp/rocketmq-eventbridge/logs

EXPOSE 7001 7002

ENTRYPOINT ["java", "-Xms256m", "-Xmx512m", "-XX:MaxDirectMemorySize=256m", "-Dspring.config.location=/etc/rocketmq-eventbridge/application.properties", "-jar", "/opt/rocketmq-eventbridge/rocketmq-eventbridge.jar"]
