package com.example.etl.rocketmq.streams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;

public class WalletStreamsPoc {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
            new TypeReference<LinkedHashMap<String, Object>>() {};

    public static void main(String[] args) throws Exception {
        String namesrv = envOrDefault("ROCKETMQ_NAMESRV", "127.0.0.1:9876");
        String sourceTopic = envOrDefault("ROCKETMQ_SOURCE_TOPIC", "wallet-events-rmq");
        String sinkTopic = envOrDefault("ROCKETMQ_SINK_TOPIC", "wallet-filtered-rmq");
        String pipelineName = envOrDefault("ROCKETMQ_PIPELINE_NAME", "rocketmq-streams-wallet-poc");

        StreamBuilder builder = new StreamBuilder("walletStreamsPocPhase2");
        builder.source(sourceTopic, bytes -> new Pair<>(null, new String(bytes, StandardCharsets.UTF_8)))
                .filter(WalletStreamsPoc::isAuthorizedPaymentCandidate)
                .map(value -> enrich(value, pipelineName))
                .sink(sinkTopic, (key, value) -> value.getBytes(StandardCharsets.UTF_8));

        TopologyBuilder topology = builder.build();
        Properties properties = new Properties();
        properties.put("rocketmq.namesrv.addr", namesrv);

        RocketMQStream stream = new RocketMQStream(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stream.stop();
            latch.countDown();
        }, "wallet-streams-poc-shutdown"));

        System.out.printf(
                "Starting RocketMQ Streams wallet PoC source=%s sink=%s namesrv=%s pipeline=%s%n",
                sourceTopic, sinkTopic, namesrv, pipelineName);
        stream.start();
        latch.await();
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? fallback : value;
    }

    private static boolean isAuthorizedPaymentCandidate(String raw) {
        if (raw == null || !raw.contains("\"event_type\":\"wallet.payment.authorized\"")) {
            return false;
        }
        try {
            LinkedHashMap<String, Object> event = MAPPER.readValue(raw, MAP_TYPE);
            return asDouble(event.get("amount")) >= 100.0;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static String enrich(String raw, String pipelineName) throws Exception {
        LinkedHashMap<String, Object> event = MAPPER.readValue(raw, MAP_TYPE);
        double amount = asDouble(event.get("amount"));
        event.put("amount", amount);
        event.put("risk_tier", amount >= 1000.0 ? "HIGH" : "STANDARD");
        event.put("pipeline", pipelineName);
        return MAPPER.writeValueAsString(event);
    }

    private static double asDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }
}
