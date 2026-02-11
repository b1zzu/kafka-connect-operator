# kafka-connect-operator

This tiny operator manages secured Kafka Connect clusters and Connectors on Kubernetes.

This operator is a lighter alternative to the more blooted Strimzi operator, and it's more flexible because it allows you to define any Kafka cluster and authentication, like AWS MSK IAM auth.

> Note: After 4 years the Strimzi operator has also introduced the option to define your custom authentication: https://github.com/strimzi/strimzi-kafka-operator/pull/11760

## Quick Starts

Prerequisites

- kind: https://github.com/kubernetes-sigs/kind

Start the Kubernetes cluster locally

```bash
kind create cluster
```

### Deploy the Operator

Deploy the kafka-connect-operator

```bash
make deploy IMG=ghcr.io/b1zzu/kafka-connect-operator:latest
```

This will deploy the `latest` version of the operator to the `kafka-connect-operator` namespace.

Follow the deployment of the operator:

```bash
kubectl get pod -n kafka-connect-operator --watch
```

You can also follow the operator’s log:

```bash
kubectl logs deployment/kafka-connect-controller-manager -n kafka-connect-operator -f
```

### Deploy an Apache Kafka Connect cluster

To run a Kafka Connect cluster you need a Kafka cluster, and the easiest way is to use Strimzi.

Deploy the Strimzi operator:

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=default' -n default
```

Deploy the Kafka cluster:

```bash
kubectl apply -f 'https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml' -n default
```

Deploy the Kafka Connect cluster:

```bash
kubectl apply -f - <<<EOF
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Cluster
metadata:
  name: my-cluster
spec:
  replicas: 1
  config:
    bootstrap.servers: my-cluster-kafka-bootstrap:9092
    key.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    group.id: connect-my-cluster
    config.storage.topic: connect-my-cluster-configs
    offset.storage.topic: connect-my-cluster-offsets
    status.storage.topic: connect-my-cluster-status
    config.storage.replication.factor: "1"
    offset.storage.replication.factor: "1"
    status.storage.replication.factor: "1"
EOF
```

Follow the deployment of the Kafka Connect cluster:

```bash
kubectl get pod -n default --watch
```

You can also follow the cluster’s log:

```bash
kubectl logs deployment/my-cluster-connect -n default -f
```

### Deploy a Connector

Deploy a connector to your Kafka Connect cluster:

```bash
kubectl apply -f - <<EOF
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Connector
metadata:
  name: my-connector
spec:
  cluster:
    name: my-cluster
  config:
    connector.class: org.apache.kafka.connect.mirror.MirrorSourceConnector
    tasks.max: "1"
    source.cluster.alias: source
    target.cluster.alias: target
    source.cluster.bootstrap.servers: my-cluster-kafka-bootstrap:9092
    target.cluster.bootstrap.servers: my-cluster-kafka-bootstrap:9092
    topics: example-a
    replication.policy.class: org.apache.kafka.connect.mirror.DefaultReplicationPolicy
    transforms: rename
    transforms.rename.type: org.apache.kafka.connect.transforms.RegexRouter
    transforms.rename.regex: topic-a
    transforms.rename.replacement: topic-b
EOF
```

Check the connector status:

```bash
kubectl describe connector my-connector -o yaml
```

## Reference

### Cluster

```yaml
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Cluster
metadata:
  name: NAME
spec:
  # Number of Kafka Connect replicas (optional, defaults to 1)
  replicas: 2

  # The Kafka Connect image to use (optional, defaults to 'docker.io/apache/kafka:latest')
  image: docker.io/apache/kafka:latest

  # Kafka Connect configuration
  # See: https://kafka.apache.org/41/configuration/kafka-connect-configs/
  config:
    # Kafka cluster connection
    bootstrap.servers: <string>

    # Converters
    key.converter: <string>
    value.converter: <string>

    # Connect cluster identification
    group.id: <string>

    # Internal topics
    config.storage.topic: <string>
    offset.storage.topic: <string>
    status.storage.topic: <string>

  # Network policy configuration (optional)
  networkPolicy:
    # Set to false to disable automatic NetworkPolicy creation
    enabled: true
```

### Connector

```yaml
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Connector
metadata:
  name: NAME
spec:
  # Reference to the Kafka Connect cluster (required)
  # Note: it must be in the same namespace
  cluster:
    name: NAME

  # Connector configuration
  # Source connectors: https://kafka.apache.org/41/configuration/kafka-connect-configs/#source-connector-configs
  # Sink connectors: https://kafka.apache.org/41/configuration/kafka-connect-configs/#sink-connector-configs
  # MirrorMaker: https://kafka.apache.org/41/configuration/mirrormaker-configs/
  config:
    # Connector class (required)
    connector.class: <string>

    # ...

```

### Network Security

The operator automatically creates NetworkPolicies for each Kafka Connect cluster to secure the REST API (port 8083). By default, only the operator and other pods in the same cluster can access the Kafka Connect API.

The operator will automatically detect the namespace where it's running to set-up the cross-namespace ingress policy.

**To disable NetworkPolicies:**
If you need to disable NetworkPolicy creation (not recommended for production), add the following to your Cluster CR:

```yaml
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Cluster
metadata:
  name: my-cluster
spec:
  networkPolicy:
    enabled: false
```

## Development

### My Workspace

This Development tutorial is based on my workspaces, so if you are facing issue try to imitate it as much as possible

- Fedora 43
- go version 1.25.7
- podman version 5.7.1
- kind version 0.31.0
- kubectl version 1.34.3
- make version 4.4.1

### Local Development

Use kind (works with Podman)

```bash
kind create cluster
```

Deploy Kafka using the Strimzi operator

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=default'
kubectl apply -f 'https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml' -n default
```

Install the CRDs

```bash
make install
```

Deploy the samples

```bash
kubectl apply -k config/samples
```

To test Connector deployments while running the controller locally you can fake the Kubernetes
by adding this to your `/etc/hosts` where `my-cluster` is the name of the Kafka Connect Cluster
and `default` the namespace where it's deployed:

```
127.0.0.1 my-cluster-connect.default
```

Then start the controller locally:

```bash
make run
```

Once the Kafka Connect cluster is ready, forward the rest port locally:

```bash
kubectl port-forward services/my-cluster-connect 8083:8083
```

The sample will deploy a Kafka Connect cluster with the name `my-cluster` when you will start the manager locally.

## Contributing

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
