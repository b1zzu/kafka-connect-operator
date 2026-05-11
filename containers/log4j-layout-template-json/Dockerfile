FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG LOG4J_VERSION=2.26.0

RUN wget -q -O /log4j-layout-template-json.jar \
      "https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-layout-template-json/${LOG4J_VERSION}/log4j-layout-template-json-${LOG4J_VERSION}.jar"

FROM scratch
COPY --from=download /log4j-layout-template-json.jar /
