FROM docker.io/confluentinc/cp-kafka-connect-base:8.2.0 AS install

ARG CONNECT_DATAGEN_VERSION=0.7.0

RUN confluent-hub install --no-prompt \
      confluentinc/kafka-connect-datagen:${CONNECT_DATAGEN_VERSION}

FROM scratch
COPY --from=install /usr/share/confluent-hub-components/confluentinc-kafka-connect-datagen/ /kafka-connect-datagen/
