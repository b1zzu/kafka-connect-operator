FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG DEBEZIUM_VERSION=3.6.1.Final
ARG AWS_JDBC_WRAPPER_VERSION=4.4.0

RUN wget -q -O /tmp/debezium-connector-postgres.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" && \
    mkdir /debezium-postgres && \
    tar -xzf /tmp/debezium-connector-postgres.tar.gz -C /debezium-postgres --strip-components=1

# Bundle AWS Advanced JDBC Wrapper into the plugin directory so the plugin's
# classloader can find it alongside the PostgreSQL JDBC driver it depends on.
RUN wget -q -O /debezium-postgres/aws-advanced-jdbc-wrapper-${AWS_JDBC_WRAPPER_VERSION}.jar \
      "https://repo1.maven.org/maven2/software/amazon/jdbc/aws-advanced-jdbc-wrapper/${AWS_JDBC_WRAPPER_VERSION}/aws-advanced-jdbc-wrapper-${AWS_JDBC_WRAPPER_VERSION}.jar"

FROM scratch
COPY --from=download /debezium-postgres/ /debezium-postgres/
