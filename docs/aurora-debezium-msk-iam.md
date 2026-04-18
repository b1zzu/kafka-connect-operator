# Debezium Connector for PostgreSQL Aurora with AWS IAM Authentication

This page is a step-by-step guide to setup:
* PostgreSQL Aurora with IAM Authentication
* Amazon MSK Serverless with IAM Authentication
* Amazon EKS with the Kafka Connect Operator

and deploy the Debezium Connector on EKS to stream all database changes to MSK.

## 1. Prerequisites

* AWS Account with permissions to create EKS, MSK, RDS, and IAM resources
* [AWS CLI](https://aws.amazon.com/cli/)
* [eksctl](https://eksctl.io/)
* [kubectl](https://kubernetes.io/docs/tasks/tools/)
* [psql](https://www.postgresql.org/download/) (PostgreSQL client)

Set your AWS region and a name prefix used throughout the tutorial:

```bash
export AWS_REGION=eu-central-1
export DEMO_NAME=kco-demo
```

## 2. Setup

### Setup Amazon EKS cluster

Create the EKS cluster:

```bash
eksctl create cluster \
  --name ${DEMO_NAME} \
  --region ${AWS_REGION} \
  --version 1.35 \
  --nodes 2 \
  --node-type m6a.large \
  --spot
```

> Kubernetes 1.35 or later is required. The operator uses [Image Volumes](https://kubernetes.io/docs/concepts/storage/volumes/#image) to mount plugin and library OCI images directly into pods. This feature is beta and enabled by default starting in Kubernetes 1.35.

This also creates a VPC with public and private subnets. The tutorial reuses this VPC for MSK and Aurora.

Configure kubectl:

```bash
aws eks update-kubeconfig --name ${DEMO_NAME} --region ${AWS_REGION}

kubectl get nodes
```

Export the VPC ID and private subnet IDs for later use:

```bash
export VPC_ID=$(aws eks describe-cluster \
  --name ${DEMO_NAME} \
  --region ${AWS_REGION} \
  --query "cluster.resourcesVpcConfig.vpcId" \
  --output text)

export PRIVATE_SUBNETS=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=${VPC_ID}" \
            "Name=tag:aws:cloudformation:logical-id,Values=SubnetPrivate*" \
  --query "Subnets[*].SubnetId" \
  --output text | tr '\t' ' ')

echo "VPC: ${VPC_ID}"
echo "Private Subnets: ${PRIVATE_SUBNETS}"
```

### Setup Amazon MSK Serverless

Create a security group for MSK:

```bash
export MSK_SG=$(aws ec2 create-security-group \
  --group-name ${DEMO_NAME}-msk \
  --description "Security group for MSK Serverless" \
  --vpc-id ${VPC_ID} \
  --query "GroupId" \
  --output text)

# Allow all traffic within the VPC (EKS nodes -> MSK)
export VPC_CIDR=$(aws ec2 describe-vpcs \
  --vpc-ids ${VPC_ID} \
  --query "Vpcs[0].CidrBlock" \
  --output text)

aws ec2 authorize-security-group-ingress \
  --group-id ${MSK_SG} \
  --protocol tcp \
  --port 9098 \
  --cidr ${VPC_CIDR}
```

Create the MSK Serverless cluster:

```bash
# Build the subnet list as JSON
export PRIVATE_SUBNETS_JSON=$(echo ${PRIVATE_SUBNETS} | tr ' ' '\n' | jq -R . | jq -sc .)

aws kafka create-cluster-v2 \
  --cluster-name ${DEMO_NAME} \
  --serverless "{
    \"VpcConfigs\": [{
      \"SubnetIds\": ${PRIVATE_SUBNETS_JSON},
      \"SecurityGroupIds\": [\"${MSK_SG}\"]
    }],
    \"ClientAuthentication\": {
      \"Sasl\": {
        \"Iam\": {
          \"Enabled\": true
        }
      }
    }
  }"
```

Wait for the cluster to become `ACTIVE`:

```bash
export MSK_CLUSTER_ARN=$(aws kafka list-clusters-v2 \
  --cluster-name-filter ${DEMO_NAME} \
  --query "ClusterInfoList[0].ClusterArn" \
  --output text)

aws kafka describe-cluster-v2 --cluster-arn ${MSK_CLUSTER_ARN} \
  --query "ClusterInfo.State" --output text
```

Get the bootstrap brokers:

```bash
export MSK_BOOTSTRAP=$(aws kafka get-bootstrap-brokers \
  --cluster-arn ${MSK_CLUSTER_ARN} \
  --query "BootstrapBrokerStringSaslIam" \
  --output text)

echo "MSK Bootstrap: ${MSK_BOOTSTRAP}"
```

### Setup the Aurora PostgreSQL cluster

Create a security group for Aurora:

```bash
export RDS_SG=$(aws ec2 create-security-group \
  --group-name ${DEMO_NAME}-aurora \
  --description "Security group for Aurora PostgreSQL" \
  --vpc-id ${VPC_ID} \
  --query "GroupId" \
  --output text)

aws ec2 authorize-security-group-ingress \
  --group-id ${RDS_SG} \
  --protocol tcp \
  --port 5432 \
  --cidr ${VPC_CIDR}
```

Create a DB subnet group:

```bash
aws rds create-db-subnet-group \
  --db-subnet-group-name ${DEMO_NAME} \
  --db-subnet-group-description "Subnets for Aurora PostgreSQL" \
  --subnet-ids ${PRIVATE_SUBNETS} # use $=PRIVATE_SUBNETS in zsh
```

Create the Aurora PostgreSQL cluster with IAM authentication enabled:

```bash
aws rds create-db-cluster \
  --db-cluster-identifier ${DEMO_NAME} \
  --engine aurora-postgresql \
  --engine-version 16.6 \
  --master-username postgres \
  --manage-master-user-password \
  --vpc-security-group-ids ${RDS_SG} \
  --db-subnet-group-name ${DEMO_NAME} \
  --enable-iam-database-authentication \
  --serverless-v2-scaling-configuration MinCapacity=0,MaxCapacity=2 \
  --database-name demo

aws rds create-db-instance \
  --db-instance-identifier ${DEMO_NAME}-instance-1 \
  --db-cluster-identifier ${DEMO_NAME} \
  --engine aurora-postgresql \
  --db-instance-class db.serverless
```

Wait for the instance to become available:

```bash
aws rds wait db-instance-available \
  --db-instance-identifier ${DEMO_NAME}-instance-1
```

Get the cluster endpoint:

```bash
export RDS_ENDPOINT=$(aws rds describe-db-clusters \
  --db-cluster-identifier ${DEMO_NAME} \
  --query "DBClusters[0].Endpoint" \
  --output text)

echo "Aurora Endpoint: ${RDS_ENDPOINT}"
```

Aurora PostgreSQL supports logical replication natively. The `rds.logical_replication` parameter must be set to `1`. Create a custom parameter group:

```bash
aws rds create-db-cluster-parameter-group \
  --db-cluster-parameter-group-name ${DEMO_NAME} \
  --db-parameter-group-family aurora-postgresql16 \
  --description "Enable logical replication"

aws rds modify-db-cluster-parameter-group \
  --db-cluster-parameter-group-name ${DEMO_NAME} \
  --parameters "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot"

aws rds modify-db-cluster \
  --db-cluster-identifier ${DEMO_NAME} \
  --db-cluster-parameter-group-name ${DEMO_NAME}

aws rds reboot-db-instance \
  --db-instance-identifier ${DEMO_NAME}-instance-1

aws rds wait db-instance-available \
  --db-instance-identifier ${DEMO_NAME}-instance-1
```

Connect to the database using the Secrets Manager master password (one-time bootstrap) to create IAM users, a demo table, and enable IAM auth for the `postgres` user:

```bash
export RDS_SECRET_ARN=$(aws rds describe-db-clusters \
  --db-cluster-identifier ${DEMO_NAME} \
  --query "DBClusters[0].MasterUserSecret.SecretArn" \
  --output text)

export RDS_MASTER_PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id ${RDS_SECRET_ARN} \
  --query "SecretString" \
  --output text | jq -r .password)

psql "host=${RDS_ENDPOINT} port=5432 dbname=demo user=postgres password=${RDS_MASTER_PASSWORD} sslmode=require" <<'SQL'
-- Create the Debezium database user
CREATE USER debezium;
GRANT rds_iam TO debezium;
GRANT rds_replication TO debezium;

-- Create a demo table
CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  customer TEXT NOT NULL,
  product TEXT NOT NULL,
  quantity INT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Grant SELECT for the initial snapshot
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- Insert sample data
INSERT INTO orders (customer, product, quantity) VALUES
  ('alice', 'widget', 5),
  ('bob', 'gadget', 3);
SQL
```

> The `rds_iam` role allows IAM authentication. The `rds_replication` role is required for Debezium to create a replication slot and read the WAL.

### Setup IAM for IRSA

Create an IAM policy that grants access to both MSK and Aurora:

```bash
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)

export RDS_RESOURCE_ID=$(aws rds describe-db-clusters \
  --db-cluster-identifier ${DEMO_NAME} \
  --query "DBClusters[0].DbClusterResourceId" \
  --output text)

cat > /tmp/${DEMO_NAME}-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "rds-db:connect",
      "Resource": [
        "arn:aws:rds-db:${AWS_REGION}:${AWS_ACCOUNT_ID}:dbuser:${RDS_RESOURCE_ID}/debezium"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:AlterCluster",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:CreateTopic",
        "kafka-cluster:AlterTopic",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData",
        "kafka-cluster:DescribeGroup",
        "kafka-cluster:AlterGroup"
      ],
      "Resource": [
        "${MSK_CLUSTER_ARN}",
        "arn:aws:kafka:${AWS_REGION}:${AWS_ACCOUNT_ID}:topic/${DEMO_NAME}/*",
        "arn:aws:kafka:${AWS_REGION}:${AWS_ACCOUNT_ID}:group/${DEMO_NAME}/*"
      ]
    }
  ]
}
EOF

aws iam create-policy \
  --policy-name ${DEMO_NAME}-kafka-connect \
  --policy-document file:///tmp/${DEMO_NAME}-policy.json
```

Create the IRSA service account using eksctl. This sets up the OIDC provider (if needed), creates the IAM role with the correct trust policy, and creates the Kubernetes ServiceAccount:

```bash
eksctl create iamserviceaccount \
  --name ${DEMO_NAME}-connect \
  --namespace default \
  --cluster ${DEMO_NAME} \
  --region ${AWS_REGION} \
  --attach-policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${DEMO_NAME}-kafka-connect \
  --approve

export IRSA_ROLE_ARN=$(kubectl get serviceaccount ${DEMO_NAME}-connect -n default \
  -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}')

echo "IRSA Role ARN: ${IRSA_ROLE_ARN}"
```

> The kafka-connect-operator creates its own ServiceAccount for each Cluster CR. We won't use the ServiceAccount created by `eksctl` directly — we only need the IAM role ARN. The operator's ServiceAccount will be annotated with this role ARN via `serviceAccountAnnotations`.

Delete the eksctl-created ServiceAccount since the operator manages its own:

```bash
kubectl delete serviceaccount ${DEMO_NAME}-connect -n default
```

## 3. Setup Kafka Connect

### Install the Kafka Connect Operator

Deploy the kafka-connect-operator:

```bash
kubectl apply -f https://github.com/b1zzu/kafka-connect-operator/releases/latest/download/install.yaml
```

Wait for the operator to be ready:

```bash
kubectl get pod -n kafka-connect-operator --watch
```

### Deploy the Kafka Connect cluster

Deploy a Kafka Connect cluster configured to connect to MSK with IAM authentication, with the Debezium PostgreSQL plugin and the AWS Advanced JDBC Wrapper library:

> Note: The `aws-advanced-jdbc-wrapper` library is packaged in the `ghcr.io/b1zzu/kafka-connect-operator/debezium-postgres` image

```bash
kubectl apply -f - <<EOF
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Cluster
metadata:
  name: ${DEMO_NAME}
spec:
  replicas: 1

  plugins:
    - name: debezium-postgres
      image: ghcr.io/b1zzu/kafka-connect-operator/debezium-postgres:latest

  libraries:
    - name: msk-iam-auth
      image: ghcr.io/b1zzu/kafka-connect-operator/msk-iam-auth:latest

  serviceAccountAnnotations:
    eks.amazonaws.com/role-arn: "${IRSA_ROLE_ARN}"

  config:
    bootstrap.servers: "${MSK_BOOTSTRAP}"
    security.protocol: SASL_SSL
    sasl.mechanism: AWS_MSK_IAM
    sasl.jaas.config: "software.amazon.msk.auth.iam.IAMLoginModule required;"
    sasl.client.callback.handler.class: "software.amazon.msk.auth.iam.IAMClientCallbackHandler"

    group.id: connect-${DEMO_NAME}
    config.storage.topic: connect-${DEMO_NAME}-configs
    offset.storage.topic: connect-${DEMO_NAME}-offsets
    status.storage.topic: connect-${DEMO_NAME}-status
    key.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
EOF
```

Wait for the Kafka Connect cluster to be ready:

```bash
kubectl get pod --watch
```

### Deploy the Debezium Connector

Deploy the Debezium PostgreSQL connector with IAM authentication:

```bash
kubectl apply -f - <<EOF
apiVersion: kafka-connect.b1zzu.net/v1alpha1
kind: Connector
metadata:
  name: ${DEMO_NAME}-cdc
spec:
  cluster:
    name: ${DEMO_NAME}
  config:
    connector.class: io.debezium.connector.postgresql.PostgresConnector
    tasks.max: "1"

    database.hostname: "${RDS_ENDPOINT}"
    database.port: "5432"
    database.user: debezium
    database.dbname: demo
    database.connection.factory.class: io.debezium.connector.postgresql.connection.PostgresAwsIamConnectionFactory

    plugin.name: pgoutput
    slot.name: ${DEMO_NAME//-/_}_slot

    topic.prefix: ${DEMO_NAME}
    schema.include.list: public
    table.include.list: public.orders
EOF
```

## 4. Verify

Check the connector status:

```bash
kubectl get connector ${DEMO_NAME}-cdc -o yaml
```

The connector should show `RUNNING` state for both the connector and its task.

Check the Kafka Connect logs to see Debezium processing the change:

```bash
kubectl logs deployment/${DEMO_NAME}-connect -f
```

## 5. Cleanup

Delete Kubernetes resources:

```bash
kubectl delete connector ${DEMO_NAME}-cdc
kubectl delete cluster ${DEMO_NAME}
kubectl delete -f https://github.com/b1zzu/kafka-connect-operator/releases/latest/download/install.yaml
```

Delete AWS resources:

```bash
# Delete the Aurora cluster
aws rds delete-db-instance \
  --db-instance-identifier ${DEMO_NAME}-instance-1 \
  --skip-final-snapshot
aws rds wait db-instance-deleted \
  --db-instance-identifier ${DEMO_NAME}-instance-1
aws rds delete-db-cluster \
  --db-cluster-identifier ${DEMO_NAME} \
  --skip-final-snapshot
aws rds delete-db-subnet-group \
  --db-subnet-group-name ${DEMO_NAME}
aws rds delete-db-cluster-parameter-group \
  --db-cluster-parameter-group-name ${DEMO_NAME}

# Delete the MSK cluster
aws kafka delete-cluster --cluster-arn ${MSK_CLUSTER_ARN}

# Delete the IAM resources
eksctl delete iamserviceaccount \
  --name ${DEMO_NAME}-connect \
  --namespace default \
  --cluster ${DEMO_NAME} \
  --region ${AWS_REGION}
aws iam delete-policy \
  --policy-arn arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${DEMO_NAME}-kafka-connect

# Delete the security groups
aws ec2 delete-security-group --group-id ${RDS_SG}
aws ec2 delete-security-group --group-id ${MSK_SG}

# Delete the EKS cluster (also deletes VPC)
eksctl delete cluster --name ${DEMO_NAME} --region ${AWS_REGION}
```
