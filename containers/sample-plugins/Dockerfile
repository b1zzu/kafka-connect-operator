FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG CONNECT_FILE_VERSION=4.2.0
ARG CONNECT_MIRROR_VERSION=4.2.0
ARG CONNECT_DATAGEN_VERSION=0.7.0

RUN wget -q -O /connect-file.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/connect-file/${CONNECT_FILE_VERSION}/connect-file-${CONNECT_FILE_VERSION}.jar" && \
    wget -q -O /connect-mirror.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/connect-mirror/${CONNECT_MIRROR_VERSION}/connect-mirror-${CONNECT_MIRROR_VERSION}.jar" && \
    wget -q -O /datagen.zip \
      "https://d2p6pa21dvn84.cloudfront.net/api/plugins/confluentinc/kafka-connect-datagen/versions/${CONNECT_DATAGEN_VERSION}/confluentinc-kafka-connect-datagen-${CONNECT_DATAGEN_VERSION}.zip" && \
    unzip -j /datagen.zip "*/lib/*" -d / && \
    rm /datagen.zip

FROM scratch
COPY --from=download /*.jar /
