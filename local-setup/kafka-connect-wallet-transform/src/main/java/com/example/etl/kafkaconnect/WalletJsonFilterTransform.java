package com.example.etl.kafkaconnect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

public class WalletJsonFilterTransform<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ConfigDef CONFIG_DEF = new ConfigDef();

  @Override
  public R apply(R record) {
    if (record == null || record.value() == null) {
      return null;
    }

    try {
      JsonNode parsed = MAPPER.readTree(valueAsString(record.value()));
      if (!parsed.isObject()) {
        return null;
      }

      ObjectNode event = (ObjectNode) parsed;
      double amount = event.path("amount").asDouble(0.0);
      boolean accepted =
          "wallet.payment.authorized".equals(event.path("event_type").asText("")) && amount >= 100.0;
      if (!accepted) {
        return null;
      }

      event.put("amount", amount);
      event.put("pipeline", "kafka-connect-k8s-phase2");
      event.put("risk_tier", amount >= 1000.0 ? "HIGH" : "STANDARD");
      event.put("processed_at_epoch_ms", Instant.now().toEpochMilli());

      String enriched = MAPPER.writeValueAsString(event);
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          Schema.STRING_SCHEMA,
          enriched,
          record.timestamp());
    } catch (Exception ignored) {
      return null;
    }
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void configure(Map<String, ?> configs) {}

  @Override
  public void close() {}

  private String valueAsString(Object value) {
    if (value instanceof byte[] bytes) {
      return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
    return value.toString();
  }
}
