# Top ETL Combination Reports

These reports expand the top five ETL combinations for the e-wallet event use case. They are separate from the candidate-level reports because each combination mixes an engine, an operations model, and a source/transform/sink deployment style.

The evidence images below were captured from actual local services on 2026-06-22 where a UI exists. StreamPark and Dinky screenshots are logged-in pages with configured jobs visible. SeaTunnel shows a real submitted Engine job row. NiFi is an unsecured local canvas with the configured flow visible. Camel does not provide a logged-in ETL workbench in this proof, so its image is labeled as runtime endpoint evidence rather than dashboard-job proof.

| Rank | Combination | Report | Can configure ETL job in UI? | Evidence image |
|---:|---|---|---|---|
| 1 | Flink Java DataStream + StreamPark | [01-flink-java-streampark.md](01-flink-java-streampark.md) | Partial: UI configures Flink app deployment/operations, not Java transform logic | [StreamPark logged-in application list](../../screenshots/live-ui/streampark-dashboard-jobs.png) |
| 2 | Flink SQL + Dinky | [02-flink-sql-dinky.md](02-flink-sql-dinky.md) | Yes, for Flink SQL jobs | [Dinky logged-in task editor](../../screenshots/live-ui/dinky-dashboard-jobs.png) |
| 3 | SeaTunnel config-managed ETL | [03-seatunnel-config.md](03-seatunnel-config.md) | Yes via separate SeaTunnel Web; current proof only shows Engine UI monitoring/running job | [SeaTunnel Engine running job](../../screenshots/live-ui/seatunnel-engine-job-created.png) |
| 4 | Apache Camel Spring Boot | [04-camel-spring.md](04-camel-spring.md) | No ETL workbench UI; configuration is code/Spring properties | [Camel Actuator endpoint](../../screenshots/live-ui/camel-actuator-routes-live.png) |
| 5 | Apache NiFi visual flow | [05-nifi-visual.md](05-nifi-visual.md) | Yes, strongest visual flow configuration | [NiFi live UI](../../screenshots/live-ui/nifi-live-ui.png) |

Main research report: [../realtime-etl-research.md](../realtime-etl-research.md)

Second-round comparison: [second-round-analysis.md](second-round-analysis.md), including runtime configuration, UI capability, screenshots, and Kafka/RocketMQ/Azure Service Bus connector support.
