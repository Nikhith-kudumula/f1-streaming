# F1 Streaming

A real-time Formula 1 data pipeline built with **Apache Flink** and **Apache Kafka**, deployed on Kubernetes using the **Strimzi** and **Flink Kubernetes Operator** patterns.

Data is sourced from the [OpenF1 API](https://openf1.org) and replayed at configurable speed for demo and testing.

## Architecture

```
OpenF1 API (replay)
      │
      ▼
 F1 Producer ──► Kafka (raw topics)
                    │
          ┌─────────┼──────────┐
          ▼         ▼          ▼
    Leaderboard  Enrichment  Alerts
    (Flink Job) (Flink Job) (Flink Job)
          │         │          │
          └─────────┼──────────┘
                    ▼
             Kafka (processed topics)
```

### Kafka Topics

| Topic | Type | Description |
|---|---|---|
| `f1.positions` | raw | Driver positions per timestamp |
| `f1.laps` | raw | Lap times per driver |
| `f1.pit_stops` | raw | Pit stop events |
| `f1.race_control` | raw | Race director messages |
| `f1.leaderboard` | processed | 10-second windowed race standings |
| `f1.laps_enriched` | processed | Lap times annotated with pit stop data |
| `f1.alerts` | processed | Safety-relevant race events (flags, safety car) |

### Flink Jobs

| Job | Class | Input → Output |
|---|---|---|
| Leaderboard | `LeaderboardJob` | `f1.positions` → `f1.leaderboard` |
| Lap Enrichment | `LapEnrichmentJob` | `f1.laps` + `f1.pit_stops` → `f1.laps_enriched` |
| Race Alerts | `RaceAlertsJob` | `f1.race_control` → `f1.alerts` |

## Prerequisites

- [minikube](https://minikube.sigs.k8s.io/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm 3](https://helm.sh/docs/intro/install/)
- [Maven 3.9+](https://maven.apache.org/)
- Java 17

## Quickstart

### 1. Start minikube

```bash
minikube start --memory=6144 --cpus=4
```

### 2. Build Docker images into minikube

```bash
mvn -B package -DskipTests
eval $(minikube docker-env)
docker build -t ghcr.io/nikhith-kudumula/f1-streaming-producer:latest ./producer
docker build -t ghcr.io/nikhith-kudumula/f1-streaming-flink-jobs:latest ./flink-jobs
```

### 3. Bootstrap everything

```bash
./scripts/bootstrap.sh
```

This installs operators, creates the Kafka cluster, topics, Flink jobs, and the producer in one shot.

### 4. Watch events flowing

```bash
# Follow producer logs
kubectl logs -f deploy/f1-producer-producer -n f1-streaming

# Consume leaderboard output
kubectl exec -it f1-kafka-kafka-0 -n f1-streaming -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic f1.leaderboard --from-beginning

# Consume alerts
kubectl exec -it f1-kafka-kafka-0 -n f1-streaming -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic f1.alerts --from-beginning
```

## Configuration

| Env Var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SESSION_KEY` | `9161` | OpenF1 session key (2023 Monaco GP Race) |
| `REPLAY_SPEED` | `2.0` | Replay multiplier (2.0 = 2× faster than real time) |

Change `SESSION_KEY` to replay a different race. Browse available sessions at `https://api.openf1.org/v1/sessions`.

## Project Structure

```
f1-streaming/
├── producer/          Java — OpenF1 poller + Kafka producer
├── flink-jobs/        Java — 3 Flink streaming jobs
├── helm/
│   ├── f1-producer/   Helm chart for producer deployment
│   └── f1-flink-jobs/ Helm chart for FlinkDeployment CRDs
├── k8s/
│   ├── operators/     Helm values for Strimzi + Flink operator
│   ├── kafka/         Strimzi KafkaCluster CRD
│   └── topics/        Strimzi KafkaTopic CRDs
└── scripts/
    └── bootstrap.sh   One-shot setup script
```
