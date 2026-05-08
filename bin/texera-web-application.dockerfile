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

# Apache Texera is an effort undergoing incubation at The Apache Software
# Foundation (ASF), sponsored by the Apache Incubator PMC. Incubation is
# required of all newly accepted projects until a further review indicates
# that the infrastructure, communications, and decision-making process have
# stabilized in a manner consistent with other successful ASF projects.
# While incubation status is not necessarily a reflection of the
# completeness or stability of the code, it does indicate that the project
# has yet to be fully endorsed by the ASF.

FROM node:24-bookworm AS build-frontend

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 build-essential git ca-certificates

WORKDIR /frontend
COPY frontend /frontend
RUN rm -f /frontend/.yarnrc.yml
RUN corepack enable && corepack prepare yarn@4.5.1 --activate && yarn set version --yarn-path 4.5.1
RUN echo "nodeLinker: node-modules" >> /frontend/.yarnrc.yml

WORKDIR /frontend
RUN yarn install && yarn run build

FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.3_2.13.11 AS build

# Set working directory
WORKDIR /texera

# Copy modules for building the service
COPY common/ common/
COPY amber/ amber/
COPY project/ project/
COPY build.sbt build.sbt

# Update system and install dependencies. python3-minimal is needed by
# bin/licensing/concat_license_binary.py below.
RUN apt-get update && apt-get install -y \
    netcat \
    unzip \
    libpq-dev \
    python3-minimal \
    && apt-get clean

# Add .git for runtime calls to jgit from OPversion
COPY .git .git
COPY LICENSE NOTICE DISCLAIMER ./
COPY licenses/ licenses/
COPY bin/licensing/ bin/licensing/

# Bring frontend/LICENSE-binary into this build stage so the per-image
# LICENSE merge below can union it with amber/LICENSE-binary-java.
COPY --from=build-frontend /frontend/LICENSE-binary amber/LICENSE-binary-frontend

RUN sbt clean WorkflowExecutionService/dist

# Unzip the texera binary
RUN unzip amber/target/universal/amber-*.zip -d amber/target/

# Merge per-aspect LICENSE-binary files (java jars + frontend npm) into a
# single LICENSE-binary-combined keyed by license group, for the runtime
# image. Per-license-group merge keeps Scala/Java jars and Angular npm
# packages inside the same Apache-2.0 / MIT / BSD / ... section instead
# of stacking the inputs end-to-end.
RUN python3 bin/licensing/concat_license_binary.py amber/LICENSE-binary-combined \
        amber/LICENSE-binary-java \
        amber/LICENSE-binary-frontend

FROM eclipse-temurin:11-jre-jammy AS runtime

WORKDIR /texera/amber
# Copy built frontend files from the build-frontend stage to match FileAssetsBundle path (../../frontend/dist from /texera/amber)
COPY --from=build-frontend /frontend/dist /frontend/dist
# Copy the built texera binary from the build phase
COPY --from=build /texera/.git /texera/amber/.git
COPY --from=build /texera/amber/target/amber-* /texera/amber/
# Copy resources directories from build phase
COPY --from=build /texera/amber/src/main/resources /texera/amber/src/main/resources
COPY --from=build /texera/common/config/src/main/resources /texera/amber/common/config/src/main/resources
# Copy ASF licensing files. LICENSE-binary and NOTICE-binary describe the
# bundled third-party contents of this image and ship as /texera/LICENSE
# and /texera/NOTICE; licenses/ holds the per-license full texts referenced
# by LICENSE-binary.
COPY --from=build /texera/amber/LICENSE-binary-combined /texera/LICENSE
COPY --from=build /texera/amber/NOTICE-binary /texera/NOTICE
COPY --from=build /texera/licenses /texera/licenses
COPY --from=build /texera/DISCLAIMER /texera/

RUN groupadd --system --gid 1001 texera \
 && useradd --system --uid 1001 --gid texera --home-dir /texera --no-create-home texera \
 && chown -R texera:texera /texera /frontend
USER texera

CMD ["bin/texera-web-application"]

EXPOSE 8080