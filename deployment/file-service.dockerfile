FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.3_2.13.11 AS build

# Set working directory
WORKDIR /core

# Copy all projects under core to /core
COPY core/ .

# Update system and install dependencies
RUN apt-get update && apt-get install -y \
    netcat \
    unzip \
    libpq-dev \
    && apt-get clean

WORKDIR /core
# Add .git for runtime calls to jgit from OPversion
COPY .git ../.git

RUN sbt clean FileService/dist

# Unzip the texera binary
RUN unzip file-service/target/universal/file-service-0.1.0.zip -d target/

FROM eclipse-temurin:11-jre-jammy AS runtime

WORKDIR /core

# Copy the built texera binary from the build phase
COPY --from=build /core/target/file-service-0.1.0 /core/
# Copy resources directories under /core from build phase
COPY --from=build /core/workflow-core/src/main/resources /core/workflow-core/src/main/resources
COPY --from=build /core/file-service/src/main/resources /core/file-service/src/main/resources

CMD ["bin/file-service"]

EXPOSE 9092