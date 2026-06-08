# Open-Source Real-Time ETL Research For E-Wallet Events

Date: 2026-06-08 MYT

Goal: find open-source real-time ETL/event-stream platforms that can subscribe to message events, filter/transform them, and publish events again. RocketMQ remains the target broker, but Kafka or another mature broker is acceptable for the first PoC.

## Executive Recommendation

Shortlist for serious PoC:

1. **Apache Flink** as the core stream-processing engine, with Kafka/Redpanda for the first local PoC and `apache/rocketmq-flink` for RocketMQ validation.
2. **Apache SeaTunnel** if the team wants config-first ETL with direct RocketMQ source/sink support and less custom Java code than Flink.
3. **Apache NiFi** if operations need a visual runtime pipeline UI, REST API, provenance, and non-developer flow editing. RocketMQ support needs custom/community connector validation.
4. **Apache Camel** for Java/Spring Boot owned routes where the event transformation belongs near application services.
5. **RocketMQ Streams / RocketMQ Connect / RocketMQ EventBridge** for RocketMQ-native routing, connector, and lightweight stream use cases, but they have smaller communities than Flink/NiFi/SeaTunnel.

My default recommendation for an e-wallet core event ETL replacement is:

**Flink + StreamPark or Dinky for operations + Kafka/Redpanda PoC first + RocketMQ connector validation second.**

Use NiFi only if low-code operations are more important than deep stream semantics. Use SeaTunnel if the needed transformations are mostly SQL/config based and direct RocketMQ source/sink matters more than custom stateful stream logic.

## Decision Summary

| Decision Need | Most Suitable Candidate | Why | Main Caveat |
|---|---|---|---|
| Default production replacement for e-wallet real-time ETL | **Apache Flink** with StreamPark or Dinky | Strongest fit for correctness, replay, state, joins/windows, backpressure, and Java team ownership | RocketMQ connector validation should be a dedicated second PoC |
| Fastest config-first RocketMQ ETL PoC | **Apache SeaTunnel** | Direct RocketMQ source/sink support and simpler config-driven jobs | Less flexible than Flink for complex stateful business logic |
| Spring Boot owned application routes | **Apache Camel** | Best match for Java/Spring Boot teams and service-owned transformation code | Runtime low-code pipeline management is weaker than NiFi/SCDF |
| Operations-managed visual flows | **Apache NiFi** | Best UI/API/provenance model for non-developer flow editing | RocketMQ processor support must be validated or built |
| RocketMQ-native lightweight processing/routing | **RocketMQ Streams, Connect, EventBridge** | Native broker fit and local RocketMQ proof exists | Smaller communities and narrower ETL capability than Flink/SeaTunnel |

For this company context, the recommended decision is **Flink as the strategic engine**, **SeaTunnel as the quickest direct-RocketMQ config-first challenger**, and **Camel as the Spring Boot fallback when the pipeline should live with application teams**.

## Local Setup Artifacts

Local PoC stack:

- Compose file: [docker-compose.kafka-poc.yml](/Users/oka/Documents/etl-research/local-setup/docker-compose.kafka-poc.yml)
- Public repo credential handling: `.env` files are ignored; tracked `.env.example` files list required local variables without real secrets. For local runs, copy each needed `.env.example` to `.env` and fill local-only values.
- Setup proof: [setup-proof.txt](/Users/oka/Documents/etl-research/local-setup/setup-proof.txt)
- Redpanda Console: `http://localhost:18080`
- Flink Dashboard: `http://localhost:18081`
- Node-RED Editor: `http://localhost:1880`
- NiFi: `https://localhost:18443/nifi`; single-user credentials are supplied through local env vars
- Kafka Connect REST: `http://localhost:18083`
- Bento health/stats: `http://localhost:14195/ready`, `http://localhost:14195/stats`
- eKuiper REST: `http://localhost:19081`
- Camel Spring Boot Actuator: `http://localhost:18084/actuator/camelroutes`
- Spark Master UI: `http://localhost:18088`
- Pulsar Admin REST: `http://localhost:18085/admin/v2/clusters`
- RocketMQ Dashboard: `http://localhost:18086`
- Beam DirectRunner script: [beam_wallet_pipeline.py](/Users/oka/Documents/etl-research/local-setup/beam-wallet-pipeline/beam_wallet_pipeline.py)
- Spring Cloud Data Flow Compose files: [local-setup/scdf](/Users/oka/Documents/etl-research/local-setup/scdf); dashboard `http://localhost:9393/dashboard` when stack is started
- StreamPark Console: `http://localhost:10000` when started from [apache-streampark_2.12-2.1.5-incubating-bin](/Users/oka/Documents/etl-research/local-setup/apache-streampark_2.12-2.1.5-incubating-bin)
- Dinky Console: `http://localhost:18888`
- Storm UI: `http://localhost:18087` from [docker-compose.storm.yml](/Users/oka/Documents/etl-research/local-setup/docker-compose.storm.yml)
- Apache InLong compose files: [local-setup/inlong](/Users/oka/Documents/etl-research/local-setup/inlong); dashboard `http://localhost/` when the stack is started

NiFi is running and `curl -k` returns the UI bundle, but the in-app browser refused its self-signed HTTPS certificate. I did not bypass that browser warning; the running-container proof is in [setup-proof.txt](/Users/oka/Documents/etl-research/local-setup/setup-proof.txt).

The in-app browser also blocked several API-only localhost ports. For those, I generated proof screenshots from live local `curl`, `docker`, and `rpk` output captured while the services were running.

Runtime screenshots:

- ![Redpanda Console topics](../screenshots/redpanda-console-topics-retry.png)
- ![Flink Dashboard](../screenshots/flink-dashboard.png)
- ![Node-RED Editor](../screenshots/node-red-editor.png)
- ![Kafka Connect runtime proof](../screenshots/kafka-connect-runtime-proof.png)
- ![Bento runtime proof](../screenshots/bento-runtime-proof.png)
- ![Camel Spring Boot runtime proof](../screenshots/camel-runtime-proof.png)
- ![eKuiper runtime proof](../screenshots/ekuiper-runtime-proof.png)
- ![Spark Master UI](../screenshots/spark-master-ui.png)
- ![Pulsar runtime proof](../screenshots/pulsar-runtime-proof.png)
- ![RocketMQ Dashboard](../screenshots/rocketmq-dashboard.png)
- ![RocketMQ Streams runtime proof](../screenshots/rocketmq-streams-runtime-proof.png)
- ![RocketMQ Connect runtime proof](../screenshots/rocketmq-connect-runtime-proof.png)
- ![RocketMQ EventBridge runtime proof](../screenshots/rocketmq-eventbridge-runtime-proof.png)
- ![Beam DirectRunner proof](../screenshots/beam-directrunner-proof.png)
- ![SeaTunnel runtime proof](../screenshots/seatunnel-runtime-proof.png)
- ![Spring Cloud Data Flow runtime proof](../screenshots/scdf-runtime-proof.png)
- ![StreamPipes runtime proof](../screenshots/streampipes-runtime-proof.png)
- ![StreamPark runtime proof](../screenshots/streampark-runtime-proof.png)
- ![Dinky runtime proof](../screenshots/dinky-runtime-proof.png)
- ![Storm runtime proof](../screenshots/storm-runtime-proof.png)
- ![InLong Dashboard](../screenshots/inlong-dashboard.png)
- ![InLong runtime proof](../screenshots/inlong-runtime-proof.png)

Candidate feature screenshots:

- Folder: [screenshots/candidates](/Users/oka/Documents/etl-research/screenshots/candidates)
- Example: ![Apache Flink card](../screenshots/candidates/01-apache-flink.png)

## Local Wallet Event PoC

The local broker uses three Kafka-compatible topics:

- `wallet-events`
- `wallet-filtered`
- `wallet-deadletter`

The local RocketMQ broker was also started for target-broker validation:

- `wallet-events-rmq`
- `wallet-filtered-rmq`

Sample input events are in [sample-wallet-events.jsonl](/Users/oka/Documents/etl-research/local-setup/proofs/sample-wallet-events.jsonl).

Verified working pipelines:

| Tool | Local Config | Result |
|---|---|---|
| Bento | [bento-wallet-pipeline.yaml](/Users/oka/Documents/etl-research/local-setup/bento-wallet-pipeline.yaml) | Consumed `wallet-events`, enriched `risk_tier`, sent authorized payments >= 100 to `wallet-filtered`, sent rejected/low-value events to `wallet-deadletter` |
| Apache Camel Spring Boot | [camel-wallet-pipeline](/Users/oka/Documents/etl-research/local-setup/camel-wallet-pipeline/pom.xml) | Built with Java 21, Spring Boot 3.5.14, Camel 4.20.0; Dockerized route is `Started` and writes the same filtered/deadletter topic pattern |
| Kafka Connect | [kafka-connect-distributed.properties](/Users/oka/Documents/etl-research/local-setup/kafka-connect-distributed.properties) | Runtime REST API is up and Connect internal topics were created; no transform connector was installed because Kafka Connect SMT is not enough for the wallet ETL by itself |
| eKuiper | Compose service `ekuiper` | Runtime REST API is up with source/sink metadata; Kafka/RocketMQ plugin validation is still needed before treating it as production-ready for this requirement |
| Apache Beam DirectRunner | [beam_wallet_pipeline.py](/Users/oka/Documents/etl-research/local-setup/beam-wallet-pipeline/beam_wallet_pipeline.py) | Ran locally in the official Beam Python SDK image; wrote filtered and deadletter JSONL outputs from the same sample wallet events |
| Apache Spark | Compose services `spark-master`, `spark-worker` | Spark 4.1.2 standalone master and one worker are up; Master UI shows `ALIVE` and one registered worker |
| Apache Pulsar | Compose service `pulsar` | Standalone broker is up; admin API returns `standalone`, healthcheck is `ok`, wallet topic has durable `proof-sub` and retained sample messages |
| Apache RocketMQ | Compose services `rocketmq-namesrv`, `rocketmq-broker`, `rocketmq-dashboard` | NameServer, broker, and dashboard are up; `mqadmin clusterList` shows `DefaultCluster` / `broker-a` and topics `wallet-events-rmq`, `wallet-filtered-rmq` exist |
| Apache RocketMQ Streams | [rocketmq-streams-wallet-poc](/Users/oka/Documents/etl-research/local-setup/rocketmq-streams-wallet-poc/pom.xml) | Built a Java SDK app with `org.apache.rocketmq:rocketmq-streams:1.1.1`; ran inside the RocketMQ Docker network; consumed `wallet-streams-events-rmq`, filtered authorized payments >= 100, enriched `risk_tier` and `pipeline`, and published two transformed records to `wallet-streams-filtered-rmq` |
| Apache RocketMQ Connect | [rocketmq-connect-standalone-local.conf](/Users/oka/Documents/etl-research/local-setup/rocketmq-connect-standalone-local.conf) | Built the official source distribution; started standalone worker REST API on `http://localhost:18082`; created `fileSourceConnector` and `fileSinkConnector` at runtime via REST; moved two records from local file to `wallet-connect-file-rmq` and back to sink file |
| Apache RocketMQ EventBridge | [rocketmq-eventbridge application.properties](/Users/oka/Documents/etl-research/local-setup/rocketmq-eventbridge-1.1.0/config/application.properties) | Ran the official 1.1.0 binary on Java 8; API started on `http://localhost:17001`; default demo bus and rule were created; `/putEvents` accepted CloudEvents with `failedEntryCount=0`; event bodies were verified in RocketMQ-backed event bus topic. The stricter phase-2 K8s pass also verified rule-based target delivery into a file sink |
| Apache InLong | Official compose files in [local-setup/inlong](/Users/oka/Documents/etl-research/local-setup/inlong) | Started the local dashboard, manager, and MySQL control-plane slice from official compose files; dashboard returned HTTP 200 and rendered the data ingestion UI. The manager container started Tomcat on 8083 but `curl` returned `Empty reply from server` and the dashboard displayed `http error` toasts under `linux/amd64` emulation, so this is setup/UI proof only, not wallet-event E2E proof |
| Spring Cloud Data Flow | Official compose files in [local-setup/scdf](/Users/oka/Documents/etl-research/local-setup/scdf) | Data Flow 2.10.2, dashboard UI 3.3.2, Skipper 2.9.2, Kafka, ZooKeeper, and MariaDB started locally; `/about`, `/runtime/apps`, Skipper `/about`, and dashboard HTML returned successfully on first boot. Repeated browser/curl requests later became slow under `linux/amd64` emulation on Apple Silicon, so the stack was stopped after proof |
| Apache StreamPipes | Official compose files in [local-setup/streampipes](/Users/oka/Documents/etl-research/local-setup/streampipes) | Full StreamPipes 0.98.0 compose stack started locally; UI returned HTTP 200 and rendered the login page, backend auto-setup completed, and extensions installed included Apache Kafka, Apache RocketMQ, Apache TubeMQ (InLong), REST, MQTT, and Pulsar. The official minimal compose path referenced a missing `backend-nats:0.98.0` image, so full compose was used for proof |
| Apache StreamPark | Local binary distribution [apache-streampark_2.12-2.1.5-incubating-bin](/Users/oka/Documents/etl-research/local-setup/apache-streampark_2.12-2.1.5-incubating-bin) | Started locally with Java 17; actuator health returned `UP`; local console login showed the Flink Application workspace, seeded Flink SQL demo, metrics, and `Add New` workflow |
| Dinky | Docker container `etl-research-dinky` | Standalone server is up on `http://localhost:18888`; actuator health returned `UP`; welcome page showed Flink SQL/Flink Jar development, deployment, and monitoring setup flow |
| Apache Storm | [docker-compose.storm.yml](/Users/oka/Documents/etl-research/local-setup/docker-compose.storm.yml) | Storm 2.8.8 cluster is up; Nimbus is `Leader`, one supervisor is registered, and UI/API are reachable on `http://localhost:18087` |
| SeaTunnel | [seatunnel-kafka-wallet.conf](/Users/oka/Documents/etl-research/local-setup/seatunnel/seatunnel-kafka-wallet.conf) | Ran with official `apache/seatunnel:latest`; read 2 fresh `wallet-events` records and wrote 1 transformed authorized-payment record to `wallet-filtered` with `pipeline=seatunnel-wallet-poc` |

## Local Evidence Coverage

| Coverage Type | Candidates |
|---|---|
| Local runtime/service execution with UI/API screenshot or proof | Flink, SeaTunnel, NiFi, InLong, Node-RED, Kafka Connect, Bento, eKuiper, Camel/Spring Boot, Spark, Pulsar, RocketMQ broker/dashboard, RocketMQ Streams, RocketMQ Connect, RocketMQ EventBridge, Beam DirectRunner, Spring Cloud Data Flow, StreamPipes, StreamPark, Dinky, Storm, Redpanda broker/console |
| Verified end-to-end wallet event filter/transform/sink | NiFi, Bento, Camel/Spring Boot, SeaTunnel, RocketMQ Streams, Beam DirectRunner |
| Verified runtime connector/source-sink movement | RocketMQ Connect |
| Verified runtime event bus/rule/API/storage/target sink | RocketMQ EventBridge |
| Local config prepared but not yet executed | None |
| Broker-only local validation plus source/card validation | None |
| Research card and source validation only | None |

This means all 20 candidates now have at least one local setup, runtime, UI, or execution proof. The strongest current local evidence remains the Kafka-compatible PoC path, the Java/Spring-owned Camel path, NiFi REST-created Kafka flow, SeaTunnel config-first ETL, the RocketMQ-native Streams path, the Beam local DirectRunner path, runtime source/sink proof for RocketMQ Connect, runtime bus/rule/storage/target sink proof for RocketMQ EventBridge, and local runtime proof for Spark/Pulsar/Spring Cloud Data Flow/StreamPipes/StreamPark/Dinky/Storm. InLong is included as UI/control-plane proof only because the manager API did not become healthy in this local run.

## Phase 2 K8s Source-Transform-Sink Proofs

The stricter phase-2 run uses Kubernetes namespace `etl-research-phase2`. Raw pod records and command outputs are in [phase2-k8s-proofs](/Users/oka/Documents/etl-research/local-setup/phase2-k8s-proofs), manifests are in [phase2-k8s](/Users/oka/Documents/etl-research/local-setup/phase2-k8s), and screenshots are in [screenshots/phase2-k8s](/Users/oka/Documents/etl-research/screenshots/phase2-k8s).

Phase-2 requirement: each candidate must run as a Kubernetes pod/job, consume from a queue, apply at least JSON conversion plus filtering/enrichment, write to a sink, record pod state while running, and include a screenshot in the report.

| Rank | Candidate | Phase 2 Status | Queue | Source -> Transform -> Sink | K8s Pod Record | Screenshot |
|---:|---|---|---|---|---|---|
| 1 | Apache Flink | Complete | Kafka/Redpanda | `phase2-flink-wallet-events-v3` -> Java DataStream JSON parse/filter/enrich -> `phase2-flink-wallet-filtered-v3` | [pods](../local-setup/phase2-k8s-proofs/01-flink-pods-running.txt), [sink](../local-setup/phase2-k8s-proofs/01-flink-filtered.txt) | ![Flink K8s proof](../screenshots/phase2-k8s/01-flink-k8s-proof.png) |
| 2 | Apache SeaTunnel | Complete | Kafka/Redpanda | `phase2-seatunnel-wallet-events-v2` -> SQL JSON filter/enrich -> `phase2-seatunnel-wallet-filtered-v2` | [pods](../local-setup/phase2-k8s-proofs/02-seatunnel-pods-running.txt), [sink](../local-setup/phase2-k8s-proofs/02-seatunnel-filtered.txt) | ![SeaTunnel K8s proof](../screenshots/phase2-k8s/02-seatunnel-k8s-proof.png) |
| 3 | Apache NiFi | Complete | Kafka/Redpanda | `phase2-nifi-wallet-events-<run-id>` -> NiFi `ConsumeKafka` -> Groovy `ExecuteScript` JSON parse/filter/enrich -> NiFi `PublishKafka` -> `phase2-nifi-wallet-filtered-<run-id>` | [pods](../local-setup/phase2-k8s-proofs/03-nifi-pods-running.txt), [flow](../local-setup/phase2-k8s-proofs/03-nifi-flow-status-summary.txt), [source](../local-setup/phase2-k8s-proofs/03-nifi-source-topic.txt), [sink](../local-setup/phase2-k8s-proofs/03-nifi-sink-topic.txt) | ![NiFi K8s proof](../screenshots/phase2-k8s/03-nifi-k8s-proof.png) |
| 4 | Apache InLong | Pending phase-2 proof | TBD | Existing proof is UI/control-plane only; manager API was unhealthy locally | Pending | Pending |
| 5 | Apache Camel | Complete | Kafka/Redpanda | `phase2-camel-wallet-events` -> Spring Boot route JSON parse/filter/enrich -> filtered/deadletter topics | [pods](../local-setup/phase2-k8s-proofs/05-camel-pods-running.txt), [filtered](../local-setup/phase2-k8s-proofs/05-camel-filtered.txt), [deadletter](../local-setup/phase2-k8s-proofs/05-camel-deadletter.txt) | ![Camel K8s proof](../screenshots/phase2-k8s/05-camel-k8s-proof.png) |
| 6 | Spring Cloud Data Flow + Spring Cloud Stream | Complete | Kafka/Redpanda | `phase2-scdf-wallet-events-<run-id>` -> SCDF Data Flow/Skipper runtime plus Spring Cloud Stream Kafka function JSON parse/filter/enrich -> `phase2-scdf-wallet-filtered-<run-id>` | [pods](../local-setup/phase2-k8s-proofs/06-scdf-pods-running.txt), [summary](../local-setup/phase2-k8s-proofs/06-scdf-summary.txt), [source](../local-setup/phase2-k8s-proofs/06-scdf-source-topic.txt), [sink](../local-setup/phase2-k8s-proofs/06-scdf-sink-topic.txt) | ![SCDF K8s proof](../screenshots/phase2-k8s/06-scdf-k8s-proof.png) |
| 7 | Apache Kafka Connect | Complete | Kafka/Redpanda | FileStreamSource `wallet-events.jsonl` -> `phase2-kafka-connect-wallet-raw-v2` -> custom Java SMT JSON parse/filter/enrich -> FileStreamSink `wallet-filtered-v2.jsonl` | [pods](../local-setup/phase2-k8s-proofs/07-kafka-connect-pods-running.txt), [status](../local-setup/phase2-k8s-proofs/07-kafka-connect-connector-status.txt), [sink](../local-setup/phase2-k8s-proofs/07-kafka-connect-filtered.txt) | ![Kafka Connect K8s proof](../screenshots/phase2-k8s/07-kafka-connect-k8s-proof.png) |
| 8 | Apache StreamPipes | Pending phase-2 proof | MQTT/Kafka likely | Existing proof is runtime/UI/extensions only; needs actual pipeline execution | Pending | Pending |
| 9 | Apache StreamPark | Complete | Kafka/Redpanda | `phase2-streampark-wallet-events-<run-id>` -> StreamPark console runtime plus Flink SQL Kafka source table JSON parse/filter/enrich -> `phase2-streampark-wallet-filtered-<run-id>` | [pods](../local-setup/phase2-k8s-proofs/09-streampark-pods-running.txt), [summary](../local-setup/phase2-k8s-proofs/09-streampark-summary.txt), [source](../local-setup/phase2-k8s-proofs/09-streampark-source-topic.txt), [sink](../local-setup/phase2-k8s-proofs/09-streampark-sink-topic.txt) | ![StreamPark K8s proof](../screenshots/phase2-k8s/09-streampark-k8s-proof.png) |
| 10 | Dinky | Complete | Kafka/Redpanda | `phase2-dinky-wallet-events-<run-id>` -> Dinky image bundled Flink SQL Kafka source table JSON parse/filter/enrich -> `phase2-dinky-wallet-filtered-<run-id>` | [pods](../local-setup/phase2-k8s-proofs/10-dinky-pods-running.txt), [summary](../local-setup/phase2-k8s-proofs/10-dinky-summary.txt), [source](../local-setup/phase2-k8s-proofs/10-dinky-source-topic.txt), [sink](../local-setup/phase2-k8s-proofs/10-dinky-sink-topic.txt) | ![Dinky K8s proof](../screenshots/phase2-k8s/10-dinky-k8s-proof.png) |
| 11 | Apache RocketMQ Streams | Complete | RocketMQ | `phase2-rocketmq-streams-wallet-events-v5` -> Java RocketMQ Streams JSON parse/filter/enrich -> `phase2-rocketmq-streams-wallet-filtered-v5` | [pods](../local-setup/phase2-k8s-proofs/11-rocketmq-streams-pods-running.txt), [source](../local-setup/phase2-k8s-proofs/11-rocketmq-streams-source.txt), [sink](../local-setup/phase2-k8s-proofs/11-rocketmq-streams-sink.txt) | ![RocketMQ Streams K8s proof](../screenshots/phase2-k8s/11-rocketmq-streams-k8s-proof.png) |
| 12 | Apache RocketMQ Connect | Complete | RocketMQ | FileSourceConnector `/data/source/wallet-events.jsonl` -> `phase2-rocketmq-connect-wallet-raw-v1` -> sink-side JSON filter/enrich transform -> FileSinkConnector `/data/sink/wallet-filtered.jsonl` | [pods](../local-setup/phase2-k8s-proofs/12-rocketmq-connect-pods-running.txt), [status](../local-setup/phase2-k8s-proofs/12-rocketmq-connect-status.json), [sink](../local-setup/phase2-k8s-proofs/12-rocketmq-connect-sink.txt) | ![RocketMQ Connect K8s proof](../screenshots/phase2-k8s/12-rocketmq-connect-k8s-proof.png) |
| 13 | Apache RocketMQ EventBridge | Complete | RocketMQ | EventBridge `/putEvents` CloudEvents -> RocketMQ-backed EventBridge bus topic -> rule filter on CloudEvent type + JSONPATH `$.data` transform -> file target `/data/sink/wallet-eventbridge-filtered.jsonl` | [pods](../local-setup/phase2-k8s-proofs/13-rocketmq-eventbridge-pods-running.txt), [source topic](../local-setup/phase2-k8s-proofs/13-rocketmq-eventbridge-eventbus-bodies.txt), [sink](../local-setup/phase2-k8s-proofs/13-rocketmq-eventbridge-sink.txt) | ![RocketMQ EventBridge K8s proof](../screenshots/phase2-k8s/13-rocketmq-eventbridge-k8s-proof.png) |
| 14 | Apache Beam | Complete | Kafka/Redpanda | `phase2-beam-wallet-events-v3` -> Beam Kafka source + Python JSON filter/enrich -> `phase2-beam-wallet-filtered-v3` | [pods](../local-setup/phase2-k8s-proofs/14-beam-pods-running.txt), [sink](../local-setup/phase2-k8s-proofs/14-beam-filtered.txt) | ![Beam K8s proof](../screenshots/phase2-k8s/14-beam-k8s-proof.png) |
| 15 | Apache Spark Structured Streaming | Complete | Kafka/Redpanda | `phase2-spark-wallet-events-v2` -> Spark JSON filter/enrich -> `phase2-spark-wallet-filtered-v2` | [pods](../local-setup/phase2-k8s-proofs/15-spark-pods-running.txt), [sink](../local-setup/phase2-k8s-proofs/15-spark-filtered.txt) | ![Spark K8s proof](../screenshots/phase2-k8s/15-spark-k8s-proof.png) |
| 16 | Apache Pulsar Functions/IO | Complete | Pulsar | `phase2-pulsar-wallet-events-v3` -> Pulsar Python Function JSON parse/filter/enrich -> `phase2-pulsar-wallet-filtered-v3` | [pods](../local-setup/phase2-k8s-proofs/16-pulsar-pods-running.txt), [status](../local-setup/phase2-k8s-proofs/16-pulsar-function-status.txt), [sink](../local-setup/phase2-k8s-proofs/16-pulsar-filtered.txt) | ![Pulsar Functions K8s proof](../screenshots/phase2-k8s/16-pulsar-k8s-proof.png) |
| 17 | Bento | Complete | Kafka/Redpanda | `phase2-bento-wallet-events` -> Bloblang JSON filter/enrich -> filtered/deadletter topics | [pods](../local-setup/phase2-k8s-proofs/17-bento-pods-running.txt), [filtered](../local-setup/phase2-k8s-proofs/17-bento-filtered.txt), [deadletter](../local-setup/phase2-k8s-proofs/17-bento-deadletter.txt) | ![Bento K8s proof](../screenshots/phase2-k8s/17-bento-k8s-proof.png) |
| 18 | LF Edge eKuiper | Complete | MQTT/Mosquitto | `phase2/ekuiper/wallet-events` -> SQL JSON filter/enrich -> filtered/deadletter MQTT topics | [pods](../local-setup/phase2-k8s-proofs/18-ekuiper-pods-running.txt), [sink](../local-setup/phase2-k8s-proofs/18-ekuiper-mqtt-output.txt) | ![eKuiper K8s proof](../screenshots/phase2-k8s/18-ekuiper-k8s-proof.png) |
| 19 | Apache Storm | Complete | Kafka/Redpanda | `phase2-storm-wallet-events` -> Storm KafkaSpout + JSON filter/enrich bolt -> KafkaBolt sink `phase2-storm-wallet-filtered` | [pods](../local-setup/phase2-k8s-proofs/19-storm-pods-running.txt), [source](../local-setup/phase2-k8s-proofs/19-storm-source.txt), [sink](../local-setup/phase2-k8s-proofs/19-storm-sink.txt) | ![Storm K8s proof](../screenshots/phase2-k8s/19-storm-k8s-proof.png) |
| 20 | Node-RED | Complete | MQTT/Mosquitto | `phase2/nodered/wallet-events` -> flow JSON parse/filter/enrich -> filtered/deadletter MQTT topics | [pods](../local-setup/phase2-k8s-proofs/20-node-red-pods-running.txt), [sink](../local-setup/phase2-k8s-proofs/20-node-red-mqtt-output.txt) | ![Node-RED K8s proof](../screenshots/phase2-k8s/20-node-red-k8s-proof.png) |

## Ranked Table

GitHub stars were collected from GitHub REST API on 2026-06-08 MYT.

| Rank | Platform | GitHub Stars | Runtime Config For New Pipeline | Learning Curve | Tech Stack Fit | MQ / RocketMQ Fit | Local Evidence | Recommendation |
|---:|---|---:|---|---|---|---|---|---|
| 1 | Apache Flink | 26,051; RocketMQ connector 172 | Medium: SQL jobs can be submitted at runtime; DataStream jobs usually redeploy JARs | High | Excellent for Java/Scala; manageable from Java teams | Kafka PoC is strong; direct RocketMQ via `apache/rocketmq-flink` source/sink | [Flink screenshot](../screenshots/flink-dashboard.png), [card](../screenshots/candidates/01-apache-flink.png) | Best core engine for stateful event ETL, windows, joins, correctness |
| 2 | Apache SeaTunnel | 9,374 | Medium: config-driven job submission; not hot-edit running logic | Medium | Java/Scala; easier config path than raw Flink | Direct RocketMQ source and sink; stream and exactly-once features documented | [runtime proof](../screenshots/seatunnel-runtime-proof.png), [config](../local-setup/seatunnel/seatunnel-kafka-wallet.conf), [card](../screenshots/candidates/02-apache-seatunnel.png) | Strong config-first direct RocketMQ ETL candidate |
| 3 | Apache NiFi | 6,111 | High: UI/API design, parameters, versioned process groups | Medium | Java backend, TypeScript UI | Kafka/JMS/HTTP are strong; no official bundled RocketMQ processor found | [card](../screenshots/candidates/03-apache-nifi.png), setup proof | Best low-code UI; validate RocketMQ connector/custom NAR risk |
| 4 | Apache InLong | 1,491 | High: manager/API model for data streams and sync jobs | High | Java/Flink ecosystem | Good real-time integration architecture; RocketMQ path should be version-validated | [runtime proof](../screenshots/inlong-runtime-proof.png), [dashboard screenshot](../screenshots/inlong-dashboard.png), [compose](../local-setup/inlong/docker-compose.yml), [card](../screenshots/candidates/04-apache-inlong.png) | Good governance platform if you want managed Flink/data integration; not a first PoC pick until manager health is validated |
| 5 | Apache Camel | 6,224 | Medium: route reload depends on runtime style | Medium | Excellent for Spring Boot teams | Direct `camel-rocketmq`; Kafka/JMS/Rabbit also mature | [runtime proof](../screenshots/camel-runtime-proof.png), [Spring Boot PoC](../local-setup/camel-wallet-pipeline/pom.xml), [card](../screenshots/candidates/05-apache-camel.png) | Best Spring-native route framework for service-owned pipelines |
| 6 | Spring Cloud Data Flow + Spring Cloud Stream | 1,141; Stream 1,069 | High: Stream DSL via UI/API | Medium | Excellent Spring Boot fit | Kafka/Rabbit native; RocketMQ via Spring Cloud Alibaba/custom binder | [runtime proof](../screenshots/scdf-runtime-proof.png), [compose](../local-setup/scdf/docker-compose.yml), [card](../screenshots/candidates/06-spring-cloud-dataflow.png) | Good if your org wants Spring Boot app pipelines as products |
| 7 | Apache Kafka Connect | 32,726 | High: REST connector create/update; SMT transforms are limited | Medium | Java | Great Kafka PoC; RocketMQ requires bridge/custom connectors | [runtime proof](../screenshots/kafka-connect-runtime-proof.png), [config](../local-setup/kafka-connect-distributed.properties), [card](../screenshots/candidates/07-apache-kafka-connect.png) | Excellent connector runtime, but pair with Flink/Camel for richer transforms |
| 8 | Apache StreamPipes | 725 | High: visual pipeline editor and runtime-installed elements | Medium | Java backend | Kafka/MQTT strong; RocketMQ and TubeMQ/InLong extensions installed in local package | [runtime proof](../screenshots/streampipes-runtime-proof.png), [browser screenshot](../screenshots/streampipes-ui-browser.png), [compose](../local-setup/streampipes/docker-compose.yml), [card](../screenshots/candidates/08-apache-streampipes.png) | Interesting self-service UI, but less proven for financial core events |
| 9 | Apache StreamPark | 4,313 | High: create/manage Flink jobs from console | Medium-High | Java/Vue; Flink ops layer | Indirect through Flink RocketMQ connector JAR | [runtime proof](../screenshots/streampark-runtime-proof.png), [local binary](../local-setup/apache-streampark_2.12-2.1.5-incubating-bin), [card](../screenshots/candidates/09-apache-streampark.png) | Good operations layer if Flink is chosen |
| 10 | Dinky | 3,738 | High: online Flink SQL dev/debug/deploy | Medium | Java/Flink | Indirect through Flink RocketMQ SQL connector | [runtime proof](../screenshots/dinky-runtime-proof.png), [card](../screenshots/candidates/10-dinky.png) | Practical Flink SQL workbench for runtime SQL iteration |
| 11 | Apache RocketMQ Streams | 179 | Low-Medium: code-first stream apps | Medium | Java | Native RocketMQ source/filter/map/sink model | [runtime proof](../screenshots/rocketmq-streams-runtime-proof.png), [Java PoC](../local-setup/rocketmq-streams-wallet-poc/pom.xml), [RocketMQ broker proof](../screenshots/rocketmq-dashboard.png), [card](../screenshots/candidates/11-rocketmq-streams.png) | Good lightweight RocketMQ-native processor; smaller community |
| 12 | Apache RocketMQ Connect | 139 | High for connector configs via REST | Medium | Java | Native RocketMQ SourceConnector/SinkConnector runtime | [runtime proof](../screenshots/rocketmq-connect-runtime-proof.png), [config](../local-setup/rocketmq-connect-standalone-local.conf), [RocketMQ broker proof](../screenshots/rocketmq-dashboard.png), [card](../screenshots/candidates/12-rocketmq-connect.png) | Best RocketMQ-native data movement; not full transform engine |
| 13 | Apache RocketMQ EventBridge | 147 | High: runtime rules, filters, transforms | Medium | Java | Native RocketMQ-backed event bus/routing patterns | [runtime proof](../screenshots/rocketmq-eventbridge-runtime-proof.png), [config](../local-setup/rocketmq-eventbridge-1.1.0/config/application.properties), [RocketMQ broker proof](../screenshots/rocketmq-dashboard.png), [card](../screenshots/candidates/13-rocketmq-eventbridge.png) | Strong event routing/filter/reshape; less for heavy ETL/state |
| 14 | Apache Beam | 8,607 | Low: code-first, redeploy for changes | High | Java/Python/Go | KafkaIO PoC; no first-class RocketMQ IO found | [runtime proof](../screenshots/beam-directrunner-proof.png), [script](../local-setup/beam-wallet-pipeline/beam_wallet_pipeline.py), [card](../screenshots/candidates/14-apache-beam.png) | Use if portability across runners matters |
| 15 | Apache Spark Structured Streaming | 43,417 | Low-Medium: jobs/notebooks/config, not low-code pipelines | Medium-High | Scala/Java/Python | Kafka source/sink standard; RocketMQ needs bridge/custom sink | [Spark UI](../screenshots/spark-master-ui.png), [card](../screenshots/candidates/15-apache-spark.png) | Good analytics engine; usually heavy for event ETL only |
| 16 | Apache Pulsar Functions/IO | 15,265 | Medium-High: CLI/API deploy/update functions/connectors | High | Java | Strong if Pulsar is broker; RocketMQ requires bridge/custom connector | [runtime proof](../screenshots/pulsar-runtime-proof.png), [card](../screenshots/candidates/16-apache-pulsar.png) | Strong platform but adds another broker decision |
| 17 | Bento | 2,000 | Medium: config/streams mode | Low-Medium | Go | Kafka/HTTP/MQTT PoC; no built-in RocketMQ found | [runtime proof](../screenshots/bento-runtime-proof.png), [config](../local-setup/bento-wallet-pipeline.yaml), [card](../screenshots/candidates/17-bento.png) | Excellent lightweight Kafka PoC engine; RocketMQ gap is real |
| 18 | LF Edge eKuiper | 1,711 | High: REST/CLI SQL rules at runtime | Low-Medium | Go/SQL | MQTT/Kafka/REST strong; RocketMQ custom source/sink plugin | [runtime proof](../screenshots/ekuiper-runtime-proof.png), [card](../screenshots/candidates/18-lf-edge-ekuiper.png) | Good lightweight rule engine, strongest at edge/IoT |
| 19 | Apache Storm | 6,685 | Low: topology redeploy for changes | High | Java | `storm-rocketmq` artifact exists; legacy spout/bolt model | [runtime proof](../screenshots/storm-runtime-proof.png), [compose](../local-setup/docker-compose.storm.yml), [card](../screenshots/candidates/19-apache-storm.png) | Technically possible but legacy; avoid unless existing expertise |
| 20 | Node-RED | 23,242 | High: visual runtime flow editing | Low | Node.js, not Java | Kafka/community nodes; RocketMQ via custom/community bridge | [Node-RED screenshot](../screenshots/node-red-editor.png), [card](../screenshots/candidates/20-node-red.png) | Good demo/prototype tool, not ideal for regulated e-wallet backbone |

## Fit Notes

### Best Production Path

For a financial/e-wallet event system, prioritize:

- Backpressure, checkpointing, replay, and exactly-once/at-least-once clarity.
- Runtime observability and controlled deployment, not only a nice flow editor.
- Java/Spring operational ownership.
- Upgrade path from Kafka PoC to RocketMQ final broker.

That makes **Flink** the safest core choice. Pair it with **StreamPark** or **Dinky** if your team wants a console for job submission and SQL development. Use **SeaTunnel** where the job is mostly movement and SQL/config transformation rather than application-specific stream logic.

### Fast PoC Path

The local PoC currently uses Redpanda as a Kafka-compatible broker:

- `wallet-events`
- `wallet-filtered`
- `wallet-deadletter`

This lets the team validate pipeline semantics with Kafka-compatible tooling first. After that, run the same use case against RocketMQ using:

- `apache/rocketmq-flink` for Flink
- SeaTunnel RocketMQ source/sink
- Camel RocketMQ component
- RocketMQ Streams for native lightweight processing
- RocketMQ EventBridge for rule-based routing/filtering

### Tools I Would Not Lead With

- **Node-RED**: useful for demos and internal tools, but not my lead choice for regulated money movement events.
- **Storm**: mature but legacy; Flink is the stronger modern replacement.
- **Pulsar**: strong platform, but it changes the broker architecture rather than only replacing ETL.
- **Beam**: good abstraction, but not enough direct RocketMQ operational value for this requirement.

## Source Links

- Apache Flink DataStream overview: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/
- RocketMQ Flink connector: https://github.com/apache/rocketmq-flink
- Apache SeaTunnel RocketMQ source: https://seatunnel.apache.org/docs/2.3.10/connector-v2/source/RocketMQ/
- Apache SeaTunnel RocketMQ sink: https://seatunnel.apache.org/docs/connectors/sink/RocketMQ/
- Apache NiFi User Guide: https://nifi.apache.org/nifi-docs/user-guide.html
- Apache InLong transform overview: https://inlong.apache.org/docs/next/modules/transform/overview/
- Apache InLong Docker compose source: https://github.com/apache/inlong/tree/master/docker/docker-compose
- Apache Camel RocketMQ component: https://camel.apache.org/components/4.18.x/rocketmq-component.html
- Spring Cloud Data Flow reference: https://docs.spring.io/spring-cloud-dataflow/docs/current/reference/htmlsingle/
- Spring Cloud Alibaba RocketMQ Binder: https://spring-cloud-alibaba-group.github.io/github-pages/2022/en-us/index.html
- Apache Kafka Connect docs: https://kafka.apache.org/documentation/#connect
- Apache StreamPipes pipeline editor: https://streampipes.apache.org/docs/use-pipeline-editor/
- Apache StreamPark introduction: https://streampark.apache.org/docs/get-started/introduction
- Dinky repository/docs entry: https://github.com/DataLinkDC/dinky
- RocketMQ Streams docs: https://rocketmq.apache.org/docs/streams/02RocketMQStreamsConcept/
- RocketMQ Connect docs: https://rocketmq.apache.org/docs/4.x/connect/02RocketMQ%20Connect%20Concept/
- RocketMQ EventBridge concepts: https://rocketmq.apache.org/docs/eventbridge/01RocketMQEventBridgeConcepts/
- Apache Beam basics: https://beam.apache.org/documentation/basics/
- Spark Structured Streaming guide: https://spark.apache.org/docs/latest/streaming/index.html
- Apache Pulsar Functions overview: https://pulsar.apache.org/docs/2.3.1/functions-overview/
- Bento repository: https://github.com/warpstreamlabs/bento
- eKuiper docs: https://ekuiper.org/docs/en/v2.1/
- Apache Storm Stream API: https://storm.apache.org/releases/2.1.0/Stream-API.html
- Node-RED repository: https://github.com/node-red/node-red
