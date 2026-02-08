# Kafka Connect Cluster Sample

## Prerequisites

### Kind

```bash
kind create cluster
```

### Kafka

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=default'
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-ephemeral.yaml
```

## Deploy

```bash
kubectl -k .
```
