# Second-Round Combination Analysis

[Back to combination index](README.md) | [Main research report](../realtime-etl-research.md)

Date: 2026-06-22 MYT

This analysis compares the top five ETL combinations through two practical questions:

1. Can the pipeline be configured at runtime without changing application code?
2. Is there a UI that can configure the ETL job or pipeline, not only monitor it?

## Summary Verdict

| Rank | Combination | Runtime config strength | UI to configure jobs? | Best fit |
|---:|---|---|---|---|
| 1 | Flink Java DataStream + StreamPark | Medium | Yes, for Flink application deployment/config; no for Java business logic | Production-grade Java/gRPC stream processing |
| 2 | Flink SQL + Dinky | High for SQL jobs | Yes | SQL-first Flink ETL authoring and deployment |
| 3 | SeaTunnel config-managed ETL | Medium | Yes via separate SeaTunnel Web; current proof only deployed Engine UI monitoring | Config-first source/transform/sink jobs |
| 4 | Apache Camel Spring Boot | Low to Medium, if engineered | No ETL workbench UI | Service-owned Java/gRPC routes |
| 5 | Apache NiFi visual flow | High | Yes | Operations-managed visual flow editing |

## Runtime Config And UI Matrix

| Combination | Runtime config? | What can be configured without code change | What still requires code/redeploy | UI to configure? | Local UI evidence |
|---|---|---|---|---|---|
| Flink Java DataStream + StreamPark | Partial | Job registration, deployment mode, Flink version, runtime parameters, environment variables, savepoint/checkpoint workflow | Java transform logic, gRPC client behavior, connector code, custom error handling | Yes for operations/deployment; not for Java logic | [StreamPark logged-in application list](../../screenshots/live-ui/streampark-dashboard-jobs.png) |
| Flink SQL + Dinky | Yes, strong for SQL | SQL source table, transform query, sink table, job execution mode, parallelism, deployment/debug settings | Custom Java/gRPC enrichment unless packaged as UDF or moved to Java job | Yes | [Dinky logged-in task editor](../../screenshots/live-ui/dinky-dashboard-jobs.png) |
| SeaTunnel config-managed ETL | Yes, config-first but redeploy-oriented | HOCON job config: source connector, transform SQL, sink connector, parallelism/checkpoint settings; SeaTunnel Web can visually create/submit tasks when deployed | Custom Java/gRPC enrichment, complex procedural logic, hot-edit of already-running job logic | Product capability: yes via separate SeaTunnel Web. Local proof: Engine UI only shows submitted/running jobs | [SeaTunnel Engine running job](../../screenshots/live-ui/seatunnel-engine-job-created.png) |
| Apache Camel Spring Boot | Limited by application design | Spring properties, ConfigMap/Secret values, route parameters if route templates are designed for it | Route DSL changes, Java processors, gRPC client code, error handling behavior | No ETL workbench UI; Actuator is runtime status only | [Camel Actuator endpoint](../../screenshots/live-ui/camel-actuator-routes-live.png) |
| Apache NiFi visual flow | Yes, strongest visual runtime control | Processors, connections, queues, parameter contexts, backpressure, start/stop, REST-managed flow updates | Custom RocketMQ processor, direct gRPC processor, compiled extension logic | Yes | [NiFi live UI canvas](../../screenshots/live-ui/nifi-live-ui.png) |

## Broker And Queue Connector Support

Assumption: "Service Bus" means **Azure Service Bus**. If it means a generic JMS/AMQP service bus product, re-check the exact vendor/protocol before choosing.

| Combination | Kafka | RocketMQ | Azure Service Bus | Custom connector or adapter needed? |
|---|---|---|---|---|
| Flink Java DataStream + StreamPark | Yes. Flink has Kafka source and sink connectors for DataStream jobs. | Yes. Use the Apache RocketMQ Flink connector source/sink and validate target Flink/RocketMQ versions. | No native official Flink Azure Service Bus connector found in the stable connector list. | Kafka/RocketMQ: no custom connector, but connector JAR/version validation is required. Azure Service Bus direct integration: yes, custom Flink Source/Sink or a bridge such as Service Bus to Kafka. |
| Flink SQL + Dinky | Yes. Flink SQL has a Kafka table connector. | Yes if the RocketMQ Flink SQL connector JAR is installed and compatible. | No native Flink SQL Azure Service Bus connector found. | Kafka/RocketMQ: no custom connector if the JARs are installed. Azure Service Bus direct integration: yes, custom DynamicTableSource/DynamicTableSink or bridge. |
| SeaTunnel config-managed ETL | Yes. SeaTunnel lists Kafka source and sink connectors. | Yes. SeaTunnel lists RocketMQ source and sink connectors. | No official SeaTunnel Azure Service Bus source/sink found in the connector lists checked. | Kafka/RocketMQ: no custom connector. Azure Service Bus direct integration: yes, custom SeaTunnel connector or bridge through Kafka/Camel/JMS/app service. |
| Apache Camel Spring Boot | Yes. Camel Kafka component supports producer and consumer routes. | Yes. Camel RocketMQ component supports producer and consumer routes. | Yes. Camel Azure ServiceBus component supports producer and consumer routes. | Usually no custom broker connector for these three. Custom Java code may still be needed for business transform, gRPC, retry, mapping, and governance. |
| Apache NiFi visual flow | Yes. NiFi has ConsumeKafka and PublishKafka processors. | Not in upstream NiFi processors checked. | Not as a direct upstream Azure Service Bus processor. Possible via JMS because Azure Service Bus supports JMS/AMQP, or via distro-specific extensions. | Kafka: no custom connector. RocketMQ direct: custom NiFi processor/NAR or adapter bridge. Azure Service Bus direct: validate JMS provider/controller-service path for the chosen NiFi distribution; otherwise custom NAR or adapter bridge. |

Practical conclusion:

| Requirement | Best fit | Reason |
|---|---|---|
| Need direct Kafka + RocketMQ + Azure Service Bus in one Java-owned implementation | Camel Spring Boot | It has direct Camel components for all three brokers. |
| Need Flink semantics and can bridge Azure Service Bus | Flink Java + StreamPark | Strongest state/checkpointing model; Kafka/RocketMQ are covered, Azure Service Bus needs bridge/custom work. |
| Need SQL/UI-first Flink and mostly Kafka/RocketMQ | Flink SQL + Dinky | Good UI authoring for SQL jobs, but Azure Service Bus is still a custom/bridge gap. |
| Need config-first source/transform/sink and Kafka/RocketMQ only | SeaTunnel | Clean connector config for Kafka/RocketMQ; Azure Service Bus needs custom/bridge work. |
| Need visual operations and Kafka only | NiFi | Strong UI and provenance; RocketMQ and Azure Service Bus are integration gaps unless extended. |

## UI Evidence Screenshots

### StreamPark

StreamPark provides a UI for Flink application registration, deployment, and operational controls. It does not let users edit Java business logic in the browser.

![StreamPark logged-in application list](../../screenshots/live-ui/streampark-dashboard-jobs.png)

### Dinky

Dinky provides a browser-based Flink SQL workbench. Users can create/edit SQL tasks that define source tables, transform SQL, and sink tables.

![Dinky logged-in task editor](../../screenshots/live-ui/dinky-dashboard-jobs.png)

### SeaTunnel

The screenshot below is SeaTunnel Engine UI, which monitors workers and submitted/running jobs. SeaTunnel also has a separate **SeaTunnel Web** project for visual task creation/submission, but that separate web console was not deployed in this local proof.

![SeaTunnel Engine running job](../../screenshots/live-ui/seatunnel-engine-job-created.png)

### Camel Spring Boot

Camel has no ETL job-authoring UI in this proof. The screenshot is a Spring Boot Actuator runtime endpoint, useful for route status only.

![Camel Actuator endpoint](../../screenshots/live-ui/camel-actuator-routes-live.png)

### NiFi

NiFi provides the strongest visual UI for creating and configuring flows, processors, connections, queues, and runtime parameters.

![NiFi live UI canvas](../../screenshots/live-ui/nifi-live-ui.png)

## Detailed Assessment

### 1. Flink Java DataStream + StreamPark

StreamPark is a Flink operations console, not the ETL engine. The runtime engine is still Flink Java DataStream.

Runtime configuration is good for deployment and operations: application registration, job parameters, savepoints, restarts, environment settings, and operational metadata can be managed outside code. It is not a low-code transform editor. If the wallet enrichment logic calls gRPC, handles retries, and controls async concurrency, that logic belongs in Java and should be changed through normal code review and JAR deployment.

UI configuration verdict: **Yes for job operations, no for business transform authoring.**

Choose this when correctness, checkpointing, replay, Java ownership, and gRPC behavior matter more than runtime low-code editing.

### 2. Flink SQL + Dinky

Dinky gives the strongest UI-based configuration path among the Flink options. A team can create and edit Flink SQL tasks in the UI, including source table DDL, transform query, sink table DDL, execution mode, and job submission settings.

Runtime configuration is high for SQL-first pipelines. The limit is custom procedural logic. Direct per-event gRPC calls from SQL are possible only through UDFs or external adapters, and that becomes harder to test and operate than Java DataStream or Camel.

UI configuration verdict: **Yes.**

Choose this when the ETL is mostly table/SQL logic and the team wants a real Flink SQL workbench.

### 3. SeaTunnel Config-Managed ETL

SeaTunnel is the cleanest config-first path. The pipeline can be reviewed as one HOCON job file with source, transform, and sink sections. That makes it strong for controlled config promotion through Git and CI.

Runtime configuration is medium: config can change without changing Java application code, but the normal model is submit/restart/redeploy a job config. There are two UI concepts:

- **SeaTunnel Engine UI:** what we deployed locally; it shows workers and submitted/running jobs.
- **SeaTunnel Web:** separate web console; official docs describe it as a visual method to create and submit SeaTunnel tasks.

UI configuration verdict: **Yes as product capability via SeaTunnel Web; not proven locally because this workspace deployed only Engine UI.**

Choose this when the company wants straightforward source/transform/sink ETL with direct connector configuration, especially if RocketMQ source/sink support is important and the transform remains simple.

### 4. Apache Camel Spring Boot

Camel Spring Boot behaves like an application service. Configuration can be externalized through Spring properties, ConfigMaps, Secrets, route templates, and feature flags, but the team must intentionally design for that. The actual transform route, Java processors, gRPC client behavior, retries, circuit breakers, and dead-letter logic are code.

Runtime configuration is therefore low to medium. It is excellent for Java/Spring ownership and normal service delivery, but it is not a platform where operators visually design pipelines.

UI configuration verdict: **No. Actuator is runtime status, not an ETL configuration UI.**

Choose this when the ETL pipeline should live with an application team and gRPC/service integration is more important than visual self-service.

### 5. Apache NiFi Visual Flow

NiFi is the strongest UI-first runtime configuration option. Operators can create processors, configure source/sink properties, connect queues, tune backpressure, start/stop components, inspect provenance, and update parameter contexts through the UI and REST API.

Runtime configuration is high, but that flexibility needs governance. For production, use parameterized flows, versioned process groups, promotion discipline, and limits around ad hoc scripts. RocketMQ and direct gRPC remain the major integration gaps unless solved through a custom processor or adapter.

UI configuration verdict: **Yes, strongest among the five.**

Choose this only if visual operations and provenance are primary requirements. For Java/gRPC-heavy enrichment, Flink Java or Camel is still stronger.

## Decision By Requirement

| Requirement | Best combination | Reason |
|---|---|---|
| Custom Java code and gRPC call in transform | Flink Java + StreamPark or Camel Spring Boot | Java owns timeout, retry, concurrency, circuit breaker, and error policy |
| SQL-configurable ETL with UI | Flink SQL + Dinky | Real UI task editor for source/transform/sink SQL |
| Config files reviewed in Git | SeaTunnel | One job config expresses source, transform, and sink clearly |
| Browser-created SeaTunnel jobs | SeaTunnel Web | Separate SeaTunnel Web console can visually create and submit SeaTunnel tasks; not the same as Engine UI |
| Runtime visual flow editing | NiFi | Strongest UI and REST model for processor/connection configuration |
| Production stream semantics and future stateful logic | Flink Java + StreamPark | Strongest checkpointing, state, replay, and scaling model |
| Service-team ownership with Spring standards | Camel Spring Boot | Fits normal Java service CI/CD and observability |

## Updated Recommendation

For the e-wallet case with possible Java/gRPC enrichment:

1. **Primary recommendation:** Flink Java DataStream + StreamPark.
2. **SQL-first alternative:** Flink SQL + Dinky.
3. **Config-first alternative:** SeaTunnel.
4. **Service-owned Java fallback:** Camel Spring Boot.
5. **Visual operations option:** NiFi.

If runtime UI configuration is mandatory, choose between **Dinky** for Flink SQL jobs, **SeaTunnel Web** for SeaTunnel config jobs, and **NiFi** for visual processor flows. If custom Java/gRPC enrichment is mandatory, prefer **Flink Java + StreamPark** or **Camel Spring Boot**, and accept that the core business logic is code, not UI configuration.

## Reference Notes

- SeaTunnel Engine UI and the screenshot in this report prove local Engine monitoring/job visibility only.
- SeaTunnel Web is a separate deployment. Official SeaTunnel Web docs describe it as the visual method to create and submit SeaTunnel tasks: https://seatunnel.apache.org/seatunnel_web/1.0.0/deploy/
- SeaTunnel's general documentation also describes two job development methods: coding and canvas design: https://seatunnel.apache.org/docs/2.3.9/about/
- Flink Kafka DataStream connector: https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/
- Flink Kafka SQL connector: https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/
- RocketMQ Flink connector: https://github.com/apache/rocketmq-flink
- SeaTunnel source connector list: https://seatunnel.apache.org/docs/connectors/source/
- SeaTunnel sink connector list: https://seatunnel.apache.org/docs/2.3.13/connectors/sink/
- Camel Kafka component: https://camel.apache.org/components/4.18.x/kafka-component.html
- Camel RocketMQ component: https://camel.apache.org/components/4.18.x/rocketmq-component.html
- Camel Azure ServiceBus component: https://camel.apache.org/components/4.18.x/azure-servicebus-component.html
- NiFi ConsumeKafka processor: https://nifi.apache.org/components/org.apache.nifi.kafka.processors.ConsumeKafka/
- NiFi PublishKafka processor: https://nifi.apache.org/components/org.apache.nifi.kafka.processors.PublishKafka/
- NiFi ConsumeJMS processor: https://nifi.apache.org/components/org.apache.nifi.jms.processors.ConsumeJMS/
- NiFi PublishJMS processor: https://nifi.apache.org/components/org.apache.nifi.jms.processors.PublishJMS/
- Azure Service Bus JMS/AMQP reference: https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-java-how-to-use-jms-api-amqp
- Confluent Azure Service Bus source connector as a bridge option into Kafka: https://docs.confluent.io/kafka-connectors/azure-servicebus/current/overview.html
