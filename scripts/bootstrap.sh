#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="f1-streaming"

echo "==> Creating namespace"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Adding Helm repos"
helm repo add strimzi https://strimzi.io/charts/
helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.9.0/
helm repo update

echo "==> Installing Strimzi Kafka Operator"
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --namespace "$NAMESPACE" \
  -f k8s/operators/strimzi/values.yaml \
  --wait --timeout 3m

echo "==> Installing Flink Kubernetes Operator"
helm upgrade --install flink-kubernetes-operator flink-operator/flink-kubernetes-operator \
  --namespace "$NAMESPACE" \
  -f k8s/operators/flink-operator/values.yaml \
  --wait --timeout 3m

echo "==> Deploying Kafka cluster"
kubectl apply -f k8s/kafka/kafka-cluster.yaml

echo "==> Waiting for Kafka to be ready..."
kubectl wait kafka/f1-kafka \
  --for=condition=Ready \
  --timeout=5m \
  -n "$NAMESPACE"

echo "==> Creating Kafka topics"
kubectl apply -f k8s/topics/topics.yaml

echo "==> Deploying Flink jobs"
helm upgrade --install f1-flink-jobs helm/f1-flink-jobs \
  --namespace "$NAMESPACE" \
  --wait --timeout 3m

echo "==> Deploying F1 producer"
helm upgrade --install f1-producer helm/f1-producer \
  --namespace "$NAMESPACE"

echo ""
echo "Bootstrap complete! Check status with:"
echo "  kubectl get pods -n $NAMESPACE"
echo "  kubectl get kafka,kafkatopics -n $NAMESPACE"
echo "  kubectl get flinkdeployment -n $NAMESPACE"
