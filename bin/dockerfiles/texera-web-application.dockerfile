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

FROM docker.io/library/node:24.19.0-bookworm@sha256:4196d66a565c6f195728d9952f161f4adfe2ad753052a08b7ec7f1c5a6bda42b AS build-frontend

ARG PACKAGE_SNAPSHOT
RUN --mount=type=bind,source=bin/dockerfiles/snapshot,target=/snapshot \
    bash /snapshot/install.sh apt --no-install-recommends python3 build-essential git ca-certificates

WORKDIR /frontend
COPY frontend /frontend
# The repo's .yarnrc.yml pins Yarn (yarnPath) and the node-modules linker;
# --immutable fails rather than re-resolving anything yarn.lock does not pin.
RUN corepack enable && corepack prepare yarn@4.14.1 --activate

# Fixes the build number build-version.js bakes into the bundle; pass the
# commit time (git log -1 --format=%ct) for a reproducible build.
ARG SOURCE_DATE_EPOCH
RUN yarn install --immutable && yarn run build

FROM docker.io/sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.5_8_1.9.3_2.13.11@sha256:c20fad6183112843d6c87fd54bb9b507a424ab7893c6a570b1624acf2e0324c4 AS build

# Set working directory
WORKDIR /texera

# Copy modules for building the service
COPY common/ common/
COPY amber/ amber/
COPY project/ project/
COPY build.sbt build.sbt
COPY .jvmopts .jvmopts

# python3-minimal is needed by bin/licensing/concat_license_binary.py;
# python3-pip installs the betterproto plugin; unzip + curl fetch protoc.
ARG PACKAGE_SNAPSHOT
RUN --mount=type=bind,source=bin/dockerfiles/snapshot,target=/snapshot \
    bash /snapshot/install.sh apt netcat unzip curl libpq-dev python3-minimal python3-pip

# Install protoc (version pinned in bin/protoc-version.txt) and the
# betterproto plugin (version pinned via amber/requirements.txt as a
# pip constraint, so the runtime base `betterproto` and the build-time
# `betterproto[compiler]` stay in lockstep), then regenerate
# amber/src/main/python/proto/ before the WorkflowExecutionService dist
# is packaged.
COPY bin/protoc-version.txt bin/protoc-version.txt
COPY bin/python-proto-gen.sh bin/python-proto-gen.sh
RUN --mount=type=bind,source=bin/dockerfiles/snapshot,target=/snapshot \
    PROTOC_VERSION=$(cat bin/protoc-version.txt) \
    && case "$(uname -m)" in \
         x86_64 | amd64) PROTOC_ARCH=x86_64 ;; \
         aarch64 | arm64) PROTOC_ARCH=aarch_64 ;; \
         *) echo "Unsupported architecture: $(uname -m)" >&2 && exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/protoc.zip "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip" \
    && unzip -o /tmp/protoc.zip -d /usr/local \
    && chmod +x /usr/local/bin/protoc \
    && rm /tmp/protoc.zip \
    && bash /snapshot/install.sh pip -c amber/requirements.txt 'betterproto[compiler]' \
    && bash bin/python-proto-gen.sh

COPY LICENSE NOTICE DISCLAIMER ./
COPY licenses/ licenses/
COPY bin/licensing/ bin/licensing/

# Bring frontend/LICENSE-binary into this build stage so the per-image
# LICENSE merge below can union it with amber/LICENSE-binary-java.
COPY --from=build-frontend /frontend/LICENSE-binary amber/LICENSE-binary-frontend

# Pins jar and dist-zip entry timestamps; pass the commit time
# (git log -1 --format=%ct) for a reproducible build.
ARG SOURCE_DATE_EPOCH
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

FROM docker.io/library/eclipse-temurin:17-jre-jammy@sha256:97137382c6f0c30427d9b7c44ad8b2d55ac823b0768c171d81a643ed219023c5 AS runtime

WORKDIR /texera/amber
# Copy built frontend files from the build-frontend stage to match FileAssetsBundle path (../../frontend/dist from /texera/amber)
COPY --from=build-frontend /frontend/dist /frontend/dist
# Copy the built texera binary from the build phase
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

# Dates the texera account in /etc/shadow; pass the commit time
# (git log -1 --format=%ct) for a reproducible build.
ARG SOURCE_DATE_EPOCH
RUN groupadd --system --gid 1001 texera \
 && useradd --system --uid 1001 --gid texera --home-dir /texera --no-create-home texera \
 && chown -R texera:texera /texera /frontend
USER texera

CMD ["bin/texera-web-application"]

EXPOSE 8080