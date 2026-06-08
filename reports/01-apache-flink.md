# Apache Flink

[Back to candidate index](README.md) | [Main report](realtime-etl-research.md)

## Recommendation

Apache Flink is the safest core engine choice for a regulated e-wallet real-time ETL backbone. It is the strongest fit where correctness, replay, checkpointing, stateful transforms, and operational maturity matter more than low-code convenience.

## Fit Summary

| Area | Assessment |
|---|---|
| Rank | 1 |
| GitHub stars | 26,051; RocketMQ connector 172 |
| Runtime config for new pipeline | Medium: SQL jobs can be submitted at runtime; DataStream jobs usually redeploy JARs |
| Learning curve | High |
| Tech stack fit | Excellent for Java/Scala teams |
| MQ / RocketMQ fit | Kafka path is strong; direct RocketMQ is available through `apache/rocketmq-flink` source/sink |

## Phase 2 Proof

| Field | Result |
|---|---|
| Status | Complete |
| Queue | Kafka/Redpanda |
| Source -> Transform -> Sink | `phase2-flink-wallet-events-v3` -> Java DataStream JSON parse/filter/enrich -> `phase2-flink-wallet-filtered-v3` |
| Pod record | [pods](../local-setup/phase2-k8s-proofs/01-flink-pods-running.txt) |
| Sink evidence | [sink](../local-setup/phase2-k8s-proofs/01-flink-filtered.txt) |

## Screenshots

![Apache Flink candidate card](../screenshots/candidates/01-apache-flink.png)

![Apache Flink K8s phase-2 proof](../screenshots/phase2-k8s/01-flink-k8s-proof.png)

![Apache Flink dashboard](../screenshots/flink-dashboard.png)

## Notes

Use Flink when wallet event processing needs stateful windows, joins, deduplication, replay, and strong delivery semantics. Pair it with StreamPark or Dinky if the team wants a management console.
