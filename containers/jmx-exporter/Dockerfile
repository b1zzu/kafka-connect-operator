FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG JMX_EXPORTER_VERSION=v1.6.0

RUN wget -q -O /jmx_prometheus_javaagent.jar \
      "https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION#v}.jar"

FROM scratch
COPY --from=download /jmx_prometheus_javaagent.jar /
