FROM flink:1.20.1-scala_2.12-java17
COPY target/flink-wallet-pipeline-0.1.0.jar /opt/flink/usrlib/flink-wallet-pipeline.jar
ENTRYPOINT ["java", "-cp", "/opt/flink/usrlib/flink-wallet-pipeline.jar:/opt/flink/lib/*", "com.example.etl.flink.WalletFlinkPipeline"]
