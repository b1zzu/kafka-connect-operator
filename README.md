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

  # The Kafka Connect image to use (optional)
  image: docker.io/apache/kafka:4.2.0

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

  # Plugins to mount from OCI images (optional)
  # Each plugin image is mounted read-only at /plugins/{name}
  # The operator automatically configures plugin.path
  plugins:
    - name: my-plugin
      image: registry.example.com/my-plugin:1.0
      pullPolicy: IfNotPresent  # optional (Always, Never, IfNotPresent)

  # Libraries to mount on the shared classpath from OCI images (optional)
  # Each library image is mounted read-only at /libraries/{name}
  # The operator automatically sets the CLASSPATH environment variable
  libraries:
    - name: msk-iam-auth
      image: ghcr.io/b1zzu/kafka-connect-operator/msk-iam-auth:2.3.5
      pullPolicy: IfNotPresent  # optional (Always, Never, IfNotPresent)

  # Secrets to mount into the pods (optional)
  # Each secret is mounted read-only at /secrets/{name}
  secrets:
    - name: my-keystore
      secretRef:
        name: my-keystore

  # Network policy configuration (optional)
  networkPolicy:
    # Set to false to disable automatic NetworkPolicy creation
    enabled: true

  # Custom annotations for the Service (optional)
  # Useful for ingress controllers and load balancer configuration.
  serviceAnnotations:
    service.beta.kubernetes.io/aws-load-balancer-type: nlb

  # Custom annotations for the ServiceAccount (optional)
  # The operator creates a dedicated ServiceAccount per Cluster.
  # Use this for e.g. AWS IRSA role bindings.
  serviceAccountAnnotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/my-role

  # Custom annotations for the Deployment (optional)
  deploymentAnnotations:
    prometheus.io/scrape: "true"

  # Custom annotations for the Pods (optional)
  # Merged with internal annotations; internal keys take precedence.
  podAnnotations:
    vault.hashicorp.com/agent-inject: "true"

  # Custom labels for the Pods (optional)
  # Applied only to the pod template, NOT to the Deployment selector.
  # Merged with internal labels; internal keys take precedence.
  podLabels:
    team: platform
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

### Plugins

Plugins listed in `spec.plugins` are mounted from OCI images as read-only volumes at `/plugins/{name}`.
The operator automatically sets `plugin.path` in the Kafka Connect configuration to include all
plugin mount directories. Do not set `plugin.path` manually when `spec.plugins` is defined.

```yaml
plugins:
  - name: debezium-postgres
    image: ghcr.io/example/debezium-postgres:2.5
  - name: s3-sink
    image: ghcr.io/example/s3-sink:1.0
    pullPolicy: Always
```

Each plugin requires:
- `name` - identifier used for the volume and mount path (`/plugins/{name}`)
- `image` - OCI image reference containing the plugin artifacts

Optional:
- `pullPolicy` - one of `Always`, `Never`, `IfNotPresent` (defaults to `Always` for `:latest` tags, `IfNotPresent` otherwise)

### Libraries

Libraries listed in `spec.libraries` are mounted from OCI images as read-only volumes at `/libraries/{name}`.
The operator automatically sets the `CLASSPATH` environment variable to include all library mount
directories (using Java wildcard syntax `/libraries/{name}/*`). The `kafka-run-class.sh` script
respects a pre-existing `CLASSPATH` and prepends it to Kafka's own classpath.

Unlike plugins (which use isolated classloaders via `plugin.path`), libraries are loaded on the
shared classpath. Use libraries for JARs that must be visible to the Kafka Connect framework itself,
such as authentication providers or custom SASL mechanisms.

```yaml
libraries:
  - name: msk-iam-auth
    image: ghcr.io/b1zzu/kafka-connect-operator/msk-iam-auth:2.3.5
```

The project provides pre-built library images for common use cases (see [Authentication](#authentication)).
You can also use any OCI image containing JAR files.

Each library requires:
- `name` - identifier used for the volume and mount path (`/libraries/{name}`)
- `image` - OCI image reference containing the library artifacts

Optional:
- `pullPolicy` - one of `Always`, `Never`, `IfNotPresent` (defaults to `Always` for `:latest` tags, `IfNotPresent` otherwise)

### Secrets

Secrets listed in `spec.secrets` are mounted read-only at `/secrets/{name}`. Use the
[FileConfigProvider](https://kafka.apache.org/41/configuration/configuration-providers/#fileconfigprovider)
(enabled by default) to reference secret files in your connector or cluster config:

```yaml
config:
  ssl.truststore.location: /secrets/my-keystore/truststore.jks
  ssl.truststore.password: "${file:/secrets/my-keystore/truststore.password:password}"
```

Secret content changes do not trigger a cluster restart.

## Authentication

### AWS MSK IAM

The [aws-msk-iam-auth](https://github.com/aws/aws-msk-iam-auth) library enables Kafka Connect
to authenticate with Amazon MSK using IAM. The project provides a pre-built image at
`ghcr.io/b1zzu/kafka-connect-operator/msk-iam-auth` that is kept up-to-date automatically.
Use `spec.libraries` to mount it and `spec.config` for the standard Kafka Connect SASL properties.

**IRSA-based authentication (recommended on EKS):**

```yaml
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Cluster
metadata:
  name: my-cluster
spec:
  libraries:
    - name: msk-iam-auth
      image: ghcr.io/b1zzu/kafka-connect-operator/msk-iam-auth:2.3.5

  serviceAccountAnnotations:
    eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/my-role"

  config:
    bootstrap.servers: b-1.my-msk-cluster.kafka.us-east-1.amazonaws.com:9098
    security.protocol: SASL_SSL
    sasl.mechanism: AWS_MSK_IAM
    sasl.jaas.config: "software.amazon.msk.auth.iam.IAMLoginModule required;"
    sasl.client.callback.handler.class: "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    # ...
```

The operator creates a dedicated ServiceAccount per Cluster. The `serviceAccountAnnotations` field
binds it to an IAM role via IRSA so that pods automatically receive temporary credentials.

The same SASL properties apply to connectors that connect to MSK-backed source or target clusters
(e.g. MirrorMaker). Set the prefixed variants (`producer.override.sasl.*`, `consumer.override.sasl.*`,
or connector-specific properties) in the Connector's `spec.config` as needed.

## Monitoring

Prometheus metrics collection is opt-in via `spec.metrics.jmxExporter`. When enabled, the operator attaches the [Prometheus JMX Exporter](https://github.com/prometheus/jmx_exporter) Java agent to the Kafka Connect process. Metrics are exposed on port `9404` at `/metrics`.

**Enable metrics:**

```yaml
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Cluster
metadata:
  name: my-cluster
spec:
  metrics:
    jmxExporter: {}
  config:
    bootstrap.servers: my-cluster-kafka-bootstrap:9092
    # ...
```

**Use a custom JMX Exporter image:**

```yaml
spec:
  metrics:
    jmxExporter:
      image: custom-registry/jmx-exporter:1.5.0
      pullPolicy: Always
```

**Prometheus pod annotations:**

```yaml
spec:
  metrics:
    jmxExporter: {}
  podAnnotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "9404"
    prometheus.io/path: "/metrics"
```

**Prometheus Operator PodMonitor:**

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: my-cluster-connect
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: kafka-connect
      app.kubernetes.io/instance: my-cluster
  podMetricsEndpoints:
    - port: metrics
      path: /metrics
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

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
