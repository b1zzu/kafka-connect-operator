FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG DEBEZIUM_VERSION=3.5.0.Final

RUN wget -q -O /tmp/debezium-connector-postgres.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" && \
    mkdir /debezium-postgres && \
    tar -xzf /tmp/debezium-connector-postgres.tar.gz -C /debezium-postgres --strip-components=1

FROM scratch
COPY --from=download /debezium-postgres/ /debezium-postgres/
