FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG CONNECT_FILE_VERSION=4.2.0
ARG CONNECT_MIRROR_VERSION=4.2.0

RUN wget -q -O /connect-file.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/connect-file/${CONNECT_FILE_VERSION}/connect-file-${CONNECT_FILE_VERSION}.jar" && \
    wget -q -O /connect-mirror.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/connect-mirror/${CONNECT_MIRROR_VERSION}/connect-mirror-${CONNECT_MIRROR_VERSION}.jar"

FROM scratch
COPY --from=download /*.jar /
