package com.example.etl.scdf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ScdfWalletStreamApplication {
  private static final Logger LOG = LoggerFactory.getLogger(ScdfWalletStreamApplication.class);
  private static final BigDecimal MIN_AUTHORIZED_AMOUNT = new BigDecimal("100.00");

  public static void main(String[] args) {
    SpringApplication.run(ScdfWalletStreamApplication.class, args);
  }

  @Bean
  Function<Flux<String>, Flux<String>> walletFilterEnrich(ObjectMapper objectMapper) {
    return input -> input.flatMap(payload -> Mono.justOrEmpty(transform(objectMapper, payload)));
  }

  private Optional<String> transform(ObjectMapper objectMapper, String payload) {
    try {
      JsonNode event = objectMapper.readTree(payload);
      String eventId = event.path("event_id").asText();
      String eventType = event.path("event_type").asText();
      BigDecimal amount = event.path("amount").decimalValue();

      if (!"wallet.payment.authorized".equals(eventType)) {
        LOG.info("Filtered event {} because event_type={}", eventId, eventType);
        return Optional.empty();
      }
      if (amount.compareTo(MIN_AUTHORIZED_AMOUNT) < 0) {
        LOG.info("Filtered event {} because amount={}", eventId, amount);
        return Optional.empty();
      }

      ObjectNode enriched = objectMapper.createObjectNode();
      enriched.put("event_id", eventId);
      enriched.put("event_type", eventType);
      enriched.put("wallet_id", event.path("wallet_id").asText());
      enriched.put("amount", amount);
      enriched.put("currency", event.path("currency").asText());
      enriched.put("risk_tier", amount.compareTo(new BigDecimal("1000.00")) >= 0 ? "HIGH" : "STANDARD");
      enriched.put("pipeline", "scdf-spring-cloud-stream-k8s-phase2");
      String output = objectMapper.writeValueAsString(enriched);
      LOG.info("Published enriched wallet event {}", output);
      return Optional.of(output);
    } catch (Exception exception) {
      LOG.warn("Dropping unparsable wallet payload: {}", payload, exception);
      return Optional.empty();
    }
  }
}
