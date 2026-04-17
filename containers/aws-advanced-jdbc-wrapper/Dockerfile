FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG AWS_JDBC_WRAPPER_VERSION=2.6.8

RUN wget -q -O /aws-advanced-jdbc-wrapper.jar \
      "https://repo1.maven.org/maven2/software/amazon/jdbc/aws-advanced-jdbc-wrapper/${AWS_JDBC_WRAPPER_VERSION}/aws-advanced-jdbc-wrapper-${AWS_JDBC_WRAPPER_VERSION}.jar"

FROM scratch
COPY --from=download /aws-advanced-jdbc-wrapper.jar /
