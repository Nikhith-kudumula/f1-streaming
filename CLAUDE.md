# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build all modules (produces fat jars in producer/target/ and flink-jobs/target/)
mvn package -DskipTests

# Build a single module
mvn package -DskipTests -pl producer
mvn package -DskipTests -pl flink-jobs

# Compile only (no jar)
mvn compile

# Clean
mvn clean
```

There are no tests yet. The shade plugin produces a single executable fat jar per module.

## Local Kubernetes Setup (minikube)

```bash
# Start minikube with enough resources for Kafka + Flink + producer
minikube start --memory=6144 --cpus=4

# Point Docker CLI at minikube's daemon (must redo this in every new shell)
eval $(minikube docker-env)

# Build images directly into minikube (avoids a registry push for local dev)
docker build -t ghcr.io/nikhith-kudumula/f1-streaming-producer:latest ./producer
docker build -t ghcr.io/nikhith-kudumula/f1-streaming-flink-jobs:latest ./flink-jobs

# Bootstrap everything (operators → Kafka → topics → Flink jobs → producer)
./scripts/bootstrap.sh

# Re-deploy after a code change
mvn package -DskipTests
eval $(minikube docker-env)
docker build -t ghcr.io/nikhith-kudumula/f1-streaming-producer:latest ./producer
helm upgrade f1-producer helm/f1-producer -n f1-streaming
```

## Watching Output

```bash
# Producer logs (replay progress)
kubectl logs -f deploy/f1-producer-producer -n f1-streaming

# Consume a processed topic
kubectl exec -it f1-kafka-kafka-0 -n f1-streaming -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic f1.leaderboard --from-beginning

# Check Flink job status
kubectl get flinkdeployment -n f1-streaming

# Check all pods
kubectl get pods -n f1-streaming
```

## Architecture

### Data Flow

```
OpenF1 API (historical session, replay mode)
    │
    ▼
producer module  ──────────────────────────────────────────────────────────
  OpenF1Client        fetches all session data once at startup via HTTP
  ReplayOrchestrator  merges all event types into one timeline, sleeps
                      between events scaled by REPLAY_SPEED
  KafkaEventPublisher serialises each event to JSON and sends to Kafka
    │
    ├──► f1.positions
    ├──► f1.laps
    ├──► f1.pit_stops
    └──► f1.race_control
         │
         ▼
flink-jobs module  ────────────────────────────────────────────────────────
  LeaderboardJob      keyBy("global") + 10s tumbling window → f1.leaderboard
  LapEnrichmentJob    CoProcessFunction: laps stream joined with pit-stop
                      ValueState (keyed by driver_number) → f1.laps_enriched
  RaceAlertsJob       ProcessFunction filter by flag/message keywords
                      → f1.alerts  (severity: CRITICAL/HIGH/MEDIUM/LOW)
```

### Key Design Decisions

**Single fat jar for flink-jobs** — all three Flink jobs live in one jar (`flink-jobs.jar`). Each `FlinkDeployment` CRD points at the same `local:///opt/flink/usrlib/flink-jobs.jar` and selects a different `entryClass`. Adding a fourth job means adding a new class + a new Helm template; no jar changes needed.

**Replay is a one-shot run** — the producer fetches all session data from OpenF1 at startup, builds a sorted timeline, then emits events with `Thread.sleep` delays. It exits when the timeline is exhausted. To replay again, restart the pod (or re-run `helm upgrade`).

**`LapEnrichmentJob` state** — pit stop data arrives on `processElement2` and is stored in `ValueState<Map<Integer,Double>>` keyed by driver number (lap_number → pit_duration). Lap events in `processElement1` look up this state. If a pit stop arrives after its lap, the enrichment will show `had_pit_stop: false` for that lap — this is a known limitation of the one-pass approach.

**Flink watermarks** — all three jobs use `forBoundedOutOfOrderness(5s)`. The timestamp is extracted from the event's `date`/`date_start` field. Events with null dates fall back to processing time.

### Configuration

All runtime config is injected via environment variables. `ProducerConfig.fromEnv()` and `FlinkJobConfig.getBootstrapServers()` read from env. Helm values flow into pod env vars; no config files are needed at runtime.

| Env Var | Default | Where used |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | producer + all Flink jobs |
| `SESSION_KEY` | `9161` (2023 Monaco GP) | producer only |
| `REPLAY_SPEED` | `2.0` | producer only |

Browse available sessions: `https://api.openf1.org/v1/sessions`

### Kubernetes / Operators

- **Strimzi** manages the `Kafka` CRD (`k8s/kafka/kafka-cluster.yaml`) and `KafkaTopic` CRDs (`k8s/topics/topics.yaml`). The in-cluster bootstrap address is `f1-kafka-kafka-bootstrap.f1-streaming.svc.cluster.local:9092`.
- **Flink Kubernetes Operator** manages `FlinkDeployment` CRDs (one per job, in `helm/f1-flink-jobs/templates/`). The Flink `ServiceAccount` with `edit` ClusterRole is created by the same chart (`serviceaccount.yaml`).
- All resources live in the `f1-streaming` namespace.

### CI

GitHub Actions (`build.yml`) runs on every push to `main`: builds with Maven, then builds and pushes both Docker images to `ghcr.io/nikhith-kudumula/`. Images are tagged with both `latest` and the commit SHA. On pull requests, only the Maven build runs (no push).
