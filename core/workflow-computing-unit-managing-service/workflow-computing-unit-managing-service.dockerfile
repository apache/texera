FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.3_2.13.11

# copy all projects under core to /core
WORKDIR /core
COPY core/ .

RUN rm -rf amber/user-resources/*
RUN apt-get update && apt-get install -y unzip

# build the service
WORKDIR /core
RUN scripts/build-services.sh

CMD ["scripts/workflow-computing-unit-managing-service.sh"]

EXPOSE 8888