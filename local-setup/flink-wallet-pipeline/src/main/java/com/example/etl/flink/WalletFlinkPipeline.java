package com.example.etl.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WalletFlinkPipeline {
  public static void main(String[] args) throws Exception {
    String brokers = env("KAFKA_BROKERS", "phase2-redpanda:9092");
    String sourceTopic = env("SOURCE_TOPIC", "phase2-flink-wallet-events");
    String sinkTopic = env("SINK_TOPIC", "phase2-flink-wallet-filtered");
    String groupId = env("GROUP_ID", "phase2-flink-wallet-poc");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(brokers)
        .setTopics(sourceTopic)
        .setGroupId(groupId)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setBounded(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers(brokers)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(sinkTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "wallet-kafka-source")
        .flatMap(new WalletFilterTransform())
        .sinkTo(sink);

    env.execute("flink-k8s-phase2-wallet-pipeline");
  }

  private static String env(String key, String fallback) {
    String value = System.getenv(key);
    return value == null || value.isBlank() ? fallback : value;
  }

  public static class WalletFilterTransform implements FlatMapFunction<String, String> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
      ObjectNode event = (ObjectNode) MAPPER.readTree(value);
      double amount = amount(event.get("amount"));
      boolean accepted =
          "wallet.payment.authorized".equals(event.path("event_type").asText()) && amount >= 100.0;
      if (!accepted) {
        return;
      }
      event.put("pipeline", "flink-k8s-phase2");
      event.put("risk_tier", amount >= 1000.0 ? "HIGH" : "STANDARD");
      out.collect(MAPPER.writeValueAsString(event));
    }

    private double amount(JsonNode node) {
      if (node == null || node.isNull()) {
        return 0.0;
      }
      return node.isNumber() ? node.asDouble() : Double.parseDouble(node.asText());
    }
  }
}
