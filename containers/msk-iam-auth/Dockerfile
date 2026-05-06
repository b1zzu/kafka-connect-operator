FROM --platform=$BUILDPLATFORM docker.io/busybox AS download

ARG MSK_IAM_AUTH_VERSION=2.3.6

RUN wget -q -O /aws-msk-iam-auth-all.jar \
      "https://github.com/aws/aws-msk-iam-auth/releases/download/v${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar"

FROM scratch
COPY --from=download /aws-msk-iam-auth-all.jar /
