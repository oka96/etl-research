package com.example.etl.storm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WalletStormTopology {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
            new TypeReference<LinkedHashMap<String, Object>>() {};

    public static void main(String[] args) throws Exception {
        String brokers = envOrDefault("KAFKA_BROKERS", "phase2-redpanda:9092");
        String sourceTopic = envOrDefault("SOURCE_TOPIC", "phase2-storm-wallet-events");
        String sinkTopic = envOrDefault("SINK_TOPIC", "phase2-storm-wallet-filtered");
        String groupId = envOrDefault("GROUP_ID", "phase2-storm-wallet-poc");
        String pipelineName = envOrDefault("PIPELINE_NAME", "storm-k8s-phase2");

        resetTopics(brokers, sourceTopic, sinkTopic);
        List<String> sourceEvents = sourceEvents();
        produceSourceEvents(brokers, sourceTopic, sourceEvents);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-source", new KafkaSpout<>(spoutConfig(brokers, sourceTopic, groupId)), 1);
        builder.setBolt("json-filter-enrich", new JsonFilterEnrichBolt(pipelineName), 1)
                .shuffleGrouping("kafka-source");
        builder.setBolt("kafka-sink", kafkaBolt(brokers, sinkTopic), 1)
                .shuffleGrouping("json-filter-enrich");

        Config config = new Config();
        config.setNumWorkers(1);
        config.setMaxSpoutPending(16);
        config.setMessageTimeoutSecs(30);
        config.put(Config.STORM_LOCAL_DIR, "/tmp/storm-local");

        System.out.printf(
                "Submitting Apache Storm topology source=%s transform=json-filter-enrich sink=%s brokers=%s%n",
                sourceTopic, sinkTopic, brokers);

        try (LocalCluster cluster = new LocalCluster()) {
            LocalCluster.LocalTopology topology =
                    cluster.submitTopology("phase2-storm-wallet-topology", config, builder.createTopology());
            List<String> sinkEvents = awaitSinkEvents(brokers, sinkTopic, 2, Duration.ofSeconds(90));
            System.out.println("Storm sink proof:");
            sinkEvents.forEach(System.out::println);
            topology.close();
        }
        long holdSeconds = Long.parseLong(envOrDefault("HOLD_SECONDS", "0"));
        if (holdSeconds > 0) {
            System.out.printf("Holding completed Storm proof pod for %d seconds so Kubernetes can record Running state%n", holdSeconds);
            Thread.sleep(TimeUnit.SECONDS.toMillis(holdSeconds));
        }
        System.out.println("Storm phase-2 proof complete");
        System.exit(0);
    }

    private static KafkaSpoutConfig<String, String> spoutConfig(String brokers, String sourceTopic, String groupId) {
        return KafkaSpoutConfig.builder(brokers, sourceTopic)
                .setGroupId(groupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
                .setRecordTranslator(record -> new Values(record.value()), new Fields("value"))
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setOffsetCommitPeriodMs(1000)
                .build();
    }

    private static KafkaBolt<String, String> kafkaBolt(String brokers, String sinkTopic) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaBolt<String, String>()
                .withProducerProperties(properties)
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"))
                .withTopicSelector(sinkTopic);
    }

    private static void resetTopics(String brokers, String sourceTopic, String sinkTopic) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        try (AdminClient admin = AdminClient.create(properties)) {
            List<String> topics = List.of(sourceTopic, sinkTopic);
            List<String> existing = topics.stream()
                    .filter(topic -> topicExists(admin, topic))
                    .collect(Collectors.toList());
            if (!existing.isEmpty()) {
                try {
                    admin.deleteTopics(existing).all().get(30, TimeUnit.SECONDS);
                } catch (ExecutionException ex) {
                    if (!(ex.getCause() instanceof UnknownTopicOrPartitionException)) {
                        throw ex;
                    }
                }
                waitForTopicsAbsent(admin, existing);
            }
            admin.createTopics(topics.stream()
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .collect(Collectors.toList()))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        }
    }

    private static boolean topicExists(AdminClient admin, String topic) {
        try {
            return admin.listTopics().names().get(30, TimeUnit.SECONDS).contains(topic);
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to list Kafka topics", ex);
        }
    }

    private static void waitForTopicsAbsent(AdminClient admin, List<String> topics) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (Collections.disjoint(admin.listTopics().names().get(10, TimeUnit.SECONDS), topics)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Kafka topics were not deleted cleanly: " + topics);
    }

    private static List<String> sourceEvents() {
        return List.of(
                "{\"event_id\":\"evt-storm-k8s-001\",\"event_type\":\"wallet.payment.authorized\",\"wallet_id\":\"wallet-storm-001\",\"amount\":250.50,\"currency\":\"MYR\",\"status\":\"authorized\"}",
                "{\"event_id\":\"evt-storm-k8s-002\",\"event_type\":\"wallet.payment.rejected\",\"wallet_id\":\"wallet-storm-002\",\"amount\":80.00,\"currency\":\"MYR\",\"status\":\"rejected\"}",
                "{\"event_id\":\"evt-storm-k8s-003\",\"event_type\":\"wallet.payment.authorized\",\"wallet_id\":\"wallet-storm-003\",\"amount\":1500.00,\"currency\":\"MYR\",\"status\":\"authorized\"}");
    }

    private static void produceSourceEvents(String brokers, String topic, List<String> events) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (String event : events) {
                String eventId = MAPPER.readValue(event, MAP_TYPE).get("event_id").toString();
                producer.send(new ProducerRecord<>(topic, eventId, event)).get(30, TimeUnit.SECONDS);
                System.out.println("Produced source event: " + event);
            }
            producer.flush();
        }
    }

    private static List<String> awaitSinkEvents(
            String brokers, String sinkTopic, int expected, Duration timeout) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "phase2-storm-proof-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        long deadline = System.nanoTime() + timeout.toNanos();
        List<String> output = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(sinkTopic));
            while (System.nanoTime() < deadline && output.size() < expected) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    output.add(record.value());
                    if (output.size() >= expected) {
                        break;
                    }
                }
            }
        }
        if (output.size() < expected) {
            throw new IllegalStateException(
                    "Expected " + expected + " sink events but found " + output.size() + ": " + output);
        }
        return output;
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? fallback : value;
    }

    public static class JsonFilterEnrichBolt extends BaseRichBolt {
        private final String pipelineName;
        private transient OutputCollector collector;

        public JsonFilterEnrichBolt(String pipelineName) {
            this.pipelineName = pipelineName;
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple input) {
            String raw = input.getStringByField("value");
            try {
                LinkedHashMap<String, Object> event = MAPPER.readValue(raw, MAP_TYPE);
                double amount = asDouble(event.get("amount"));
                boolean accepted = "wallet.payment.authorized".equals(String.valueOf(event.get("event_type")))
                        && amount >= 100.0;
                if (accepted) {
                    event.put("amount", amount);
                    event.put("risk_tier", amount >= 1000.0 ? "HIGH" : "STANDARD");
                    event.put("pipeline", pipelineName);
                    collector.emit(input, new Values(event.get("event_id").toString(), MAPPER.writeValueAsString(event)));
                }
                collector.ack(input);
            } catch (Exception ex) {
                collector.reportError(ex);
                collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "message"));
        }

        private static double asDouble(Object value) {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return Double.parseDouble(String.valueOf(value));
        }
    }
}
