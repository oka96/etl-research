package com.example.walletetl;

import java.time.Instant;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

@Component
public class WalletEventRoute extends RouteBuilder {
  @Override
  public void configure() {
    from("kafka:wallet-events"
        + "?brokers={{wallet.kafka.brokers}}"
        + "&groupId={{wallet.kafka.group-id}}"
        + "&autoOffsetReset=earliest")
      .routeId("camel-wallet-filter-transform")
      .unmarshal().json(JsonLibrary.Jackson, Map.class)
      .process(this::enrichAndChooseTargetTopic)
      .marshal().json(JsonLibrary.Jackson)
      .toD("kafka:${exchangeProperty.targetTopic}?brokers={{wallet.kafka.brokers}}");
  }

  private void enrichAndChooseTargetTopic(Exchange exchange) {
    @SuppressWarnings("unchecked")
    Map<String, Object> event = exchange.getMessage().getBody(Map.class);

    double amount = asDouble(event.get("amount"));
    boolean accepted =
      "wallet.payment.authorized".equals(event.get("event_type")) && amount >= 100.0;

    event.put("pipeline", "camel-spring-wallet-poc");
    event.put("processed_at", Instant.now().toString());
    event.put("risk_tier", amount >= 1000.0 ? "HIGH" : "STANDARD");

    exchange.getMessage().setBody(event);
    exchange.setProperty("targetTopic", accepted ? "wallet-filtered" : "wallet-deadletter");
  }

  private double asDouble(Object value) {
    if (value instanceof Number number) {
      return number.doubleValue();
    }
    if (value instanceof String text && !text.isBlank()) {
      return Double.parseDouble(text);
    }
    return 0.0;
  }
}
