# Use to build workflow-pod-brain image

FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.3_2.13.11

WORKDIR /core/workflow-pod-brain
COPY core/workflow-pod-brain .
RUN sbt clean package
RUN apt-get update
RUN apt-get install -y netcat unzip

WORKDIR /core
COPY core/scripts ./scripts
# Add .git for runtime calls to jgit from OPversion
COPY .git ../.git

RUN scripts/build-brain.sh

CMD ["scripts/deploy-brain.sh"]

EXPOSE 8888
