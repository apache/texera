# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
RUN unzip amber/target/universal/texera-*.zip -d amber/target/

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
COPY --from=build /core/amber/target/texera-* /core/amber
# Copy resources directories under /core from build phase
COPY --from=build /core/amber/src/main/resources /core/amber/src/main/resources
COPY --from=build /core/config/src/main/resources /core/config/src/main/resources

CMD ["bin/computing-unit-worker"]

EXPOSE 8085