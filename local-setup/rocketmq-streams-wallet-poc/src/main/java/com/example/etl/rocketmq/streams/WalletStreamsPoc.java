package com.example.etl.rocketmq.streams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WalletStreamsPoc {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE = new TypeReference<LinkedHashMap<String, Object>>() {
    };

    public static void main(String[] args) throws Exception {
        String namesrv = envOrDefault("ROCKETMQ_NAMESRV", "127.0.0.1:9876");
        String sourceTopic = envOrDefault("ROCKETMQ_SOURCE_TOPIC", "wallet-events-rmq");
        String sinkTopic = envOrDefault("ROCKETMQ_SINK_TOPIC", "wallet-filtered-rmq");

        StreamBuilder builder = new StreamBuilder("walletStreamsPoc");
        builder.source(sourceTopic, payload -> new Pair<>(null, new String(payload, StandardCharsets.UTF_8)))
                .filter(WalletStreamsPoc::isAuthorizedPaymentCandidate)
                .map(WalletStreamsPoc::enrich)
                .sink(sinkTopic, (key, value) -> value.getBytes(StandardCharsets.UTF_8));

        TopologyBuilder topologyBuilder = builder.build();
        Properties properties = new Properties();
        properties.put(MixAll.NAMESRV_ADDR_PROPERTY, namesrv);

        RocketMQStream stream = new RocketMQStream(topologyBuilder, properties);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stream.stop();
            latch.countDown();
        }, "wallet-streams-poc-shutdown"));

        System.out.printf("Starting RocketMQ Streams wallet PoC source=%s sink=%s namesrv=%s%n", sourceTopic, sinkTopic, namesrv);
        stream.start();
        latch.await();
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? defaultValue : value;
    }

    private static boolean isAuthorizedPaymentCandidate(String value) {
        if (value == null
                || !value.contains("\"event_type\":\"wallet.payment.authorized\"")
                || value.contains("\"status\":\"rejected\"")) {
            return false;
        }
        try {
            Map<String, Object> event = MAPPER.readValue(value, MAP_TYPE);
            return asDouble(event.get("amount")) >= 100.0d;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static String enrich(String value) throws Exception {
        Map<String, Object> event = MAPPER.readValue(value, MAP_TYPE);
        double amount = asDouble(event.get("amount"));
        event.put("risk_tier", amount >= 1000.0d ? "HIGH" : "STANDARD");
        event.put("pipeline", "rocketmq-streams-wallet-poc");
        return MAPPER.writeValueAsString(event);
    }

    private static double asDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }
}
