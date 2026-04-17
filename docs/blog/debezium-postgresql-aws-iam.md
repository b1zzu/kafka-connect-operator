# Debezium Connector for PostgreSQL with AWS IAM Authentication

Debezium 3.4 added `PostgresAwsIamConnectionFactory`, which lets Debezium authenticate to PostgreSQL (including Aurora) using AWS IAM instead of a static password. It requires the [AWS Advanced JDBC Wrapper](https://github.com/aws/aws-advanced-jdbc-wrapper) on the classpath. Here's how to set it up.

## IAM Credentials

**Static keys:** Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` as environment variables on the Kafka Connect worker. Simple, but you have to rotate them yourself.

**IRSA (recommended on EKS):** Attach an IAM role to the Kafka Connect pod via a Kubernetes ServiceAccount. The pod gets temporary credentials automatically — no secrets to manage.

## IRSA Setup

For the full picture on how IRSA works (creating the OIDC provider, associating it with your cluster, etc.), see the [AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html).

### IAM Role Trust Policy

Allow the EKS OIDC provider to assume the role, and grant `rds-db:connect` so the role can generate IAM database tokens:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::123456789012:oidc-provider/oidc.eks.eu-central-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "oidc.eks.eu-central-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:sub": "system:serviceaccount:MY_NAMESPACE:MY_SERVICE_ACCOUNT"
        }
      }
    }
  ]
}
```

Attach an inline or managed policy with `rds-db:connect`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "rds-db:connect",
      "Resource": "arn:aws:rds-db:eu-central-1:123456789012:dbuser:CLUSTER_RESOURCE_ID/DB_USER"
    }
  ]
}
```

### Kubernetes ServiceAccount

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: MY_SERVICE_ACCOUNT
  namespace: MY_NAMESPACE
  annotations:
    eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/MY_ROLE"
```

### Kubernetes Pod Spec

```yaml
spec:
  serviceAccountName: MY_SERVICE_ACCOUNT
  containers:
    - name: kafka-connect
      # ...
```

EKS automatically injects `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` into the pod.

## Dockerfile

A single image based on `docker.io/apache/kafka` that bundles the Debezium PostgreSQL connector and the AWS Advanced JDBC Wrapper:

```dockerfile
FROM docker.io/apache/kafka:4.2.0

# Debezium PostgreSQL connector plugin
# Distributed as a tar.gz bundle (connector JAR + transitive dependencies), so it must be extracted
ADD https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.1.1.Final/debezium-connector-postgres-3.1.1.Final-plugin.tar.gz /tmp/dbz.tar.gz
RUN mkdir -p /opt/kafka/plugins/debezium-postgres && \
    tar -xzf /tmp/dbz.tar.gz -C /opt/kafka/plugins/debezium-postgres --strip-components=1 && \
    rm /tmp/dbz.tar.gz

# AWS Advanced JDBC Wrapper — single standalone JAR, added directly
# Must be on the shared classpath, not inside the plugin dir
ADD https://repo1.maven.org/maven2/software/amazon/jdbc/aws-advanced-jdbc-wrapper/2.5.6/aws-advanced-jdbc-wrapper-2.5.6.jar /opt/kafka/libs/
```

> The JDBC wrapper goes in `/opt/kafka/libs/` (shared classpath) because `PostgresAwsIamConnectionFactory` needs it visible to the framework classloader, not the plugin's isolated classloader.

## Debezium Connector Config

```json
{
  "name": "my-cdc-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "my-db.cluster-abc123.eu-central-1.rds.amazonaws.com",
    "database.port": "5432",
    "database.user": "my_iam_user",
    "database.dbname": "my_database",
    "database.connection.factory.class": "io.debezium.connector.postgresql.connection.PostgresAwsIamConnectionFactory",

    "plugin.name": "pgoutput",
    "slot.name": "my_slot",

    "topic.prefix": "my_cdc",
    "schema.include.list": "public",
    "table.include.list": "public.my_table"
  }
}
```

No `database.password` — IAM handles authentication. The `database.user` must match the IAM database user in your `rds-db:connect` policy.

## What's Next — kafka-connect-operator

The [kafka-connect-operator](https://github.com/b1zzu/kafka-connect-operator) manages Kafka Connect clusters and connectors as Kubernetes CRDs. Instead of building a monolithic Docker image, you ship plugins and libraries as minimal OCI images and the operator mounts them at runtime.

### Debezium Plugin OCI Image

```dockerfile
FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG DEBEZIUM_POSTGRES_VERSION=3.1.1.Final

RUN wget -q -O /tmp/dbz.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_POSTGRES_VERSION}/debezium-connector-postgres-${DEBEZIUM_POSTGRES_VERSION}-plugin.tar.gz" && \
    mkdir -p /plugin && \
    tar -xzf /tmp/dbz.tar.gz -C /plugin --strip-components=1

FROM scratch
COPY --from=download /plugin/ /
```

### AWS Advanced JDBC Wrapper OCI Image

```dockerfile
FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG AWS_JDBC_WRAPPER_VERSION=2.5.6

RUN wget -q -O /aws-advanced-jdbc-wrapper.jar \
      "https://repo1.maven.org/maven2/software/amazon/jdbc/aws-advanced-jdbc-wrapper/${AWS_JDBC_WRAPPER_VERSION}/aws-advanced-jdbc-wrapper-${AWS_JDBC_WRAPPER_VERSION}.jar"

FROM scratch
COPY --from=download /aws-advanced-jdbc-wrapper.jar /
```

### Cluster CR

```yaml
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Cluster
metadata:
  name: my-cluster
spec:
  replicas: 2

  plugins:
    - name: debezium-postgres
      image: registry.example.com/debezium-postgres:3.1.1.Final

  libraries:
    - name: aws-advanced-jdbc-wrapper
      image: registry.example.com/aws-advanced-jdbc-wrapper:2.5.6

  serviceAccountAnnotations:
    eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/MY_ROLE"

  config:
    bootstrap.servers: b-1.my-msk-cluster.kafka.eu-central-1.amazonaws.com:9098
    security.protocol: SASL_SSL
    sasl.mechanism: AWS_MSK_IAM
    sasl.jaas.config: "software.amazon.msk.auth.iam.IAMLoginModule required;"
    sasl.client.callback.handler.class: "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    group.id: connect-my-cluster
    config.storage.topic: connect-my-cluster-configs
    offset.storage.topic: connect-my-cluster-offsets
    status.storage.topic: connect-my-cluster-status
    key.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
```

### Connector CR

```yaml
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Connector
metadata:
  name: my-cdc-source
spec:
  cluster:
    name: my-cluster
  config:
    connector.class: io.debezium.connector.postgresql.PostgresConnector
    tasks.max: "1"
    database.hostname: my-db.cluster-abc123.eu-central-1.rds.amazonaws.com
    database.port: "5432"
    database.user: my_iam_user
    database.dbname: my_database
    database.connection.factory.class: io.debezium.connector.postgresql.connection.PostgresAwsIamConnectionFactory
    plugin.name: pgoutput
    slot.name: my_slot
    topic.prefix: my_cdc
    schema.include.list: public
    table.include.list: public.my_table
```
