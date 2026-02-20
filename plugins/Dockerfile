FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG KAFKA_VERSION=4.0.0

RUN wget -q -O /connect-file.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/connect-file/${KAFKA_VERSION}/connect-file-${KAFKA_VERSION}.jar" && \
    wget -q -O /connect-mirror.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/connect-mirror/${KAFKA_VERSION}/connect-mirror-${KAFKA_VERSION}.jar"

FROM scratch
COPY --from=download /connect-*.jar /
