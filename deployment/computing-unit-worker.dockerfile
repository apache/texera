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

RUN sbt clean WorkflowExecutionService/dist

# Unzip the texera binary
RUN unzip amber/target/universal/texera-0.1-SNAPSHOT.zip -d amber/target/

FROM eclipse-temurin:11-jre-jammy AS runtime

WORKDIR /core/amber

COPY --from=build /core/amber/requirements.txt /tmp/requirements.txt
COPY --from=build /core/amber/operator-requirements.txt /tmp/operator-requirements.txt

# Install Python runtime and dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    libpq-dev \
    && apt-get clean

RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install python-lsp-server python-lsp-server[websockets]

# Install requirements with a fallback for wordcloud
RUN pip3 install -r /tmp/requirements.txt
RUN pip3 install --no-cache-dir --find-links https://pypi.org/simple/ -r /tmp/operator-requirements.txt || \
    pip3 install --no-cache-dir wordcloud==1.9.2

# Copy the built texera binary from the build phase
COPY --from=build /core/amber/target/texera-0.1-SNAPSHOT /core/amber
# Copy resources directories under /core from build phase
COPY --from=build /core/amber/src/main/resources /core/amber/src/main/resources
COPY --from=build /core/workflow-core/src/main/resources /core/workflow-core/src/main/resources
COPY --from=build /core/file-service/src/main/resources /core/file-service/src/main/resources

CMD ["bin/computing-unit-worker"]

EXPOSE 8085