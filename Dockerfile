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

FROM node:18-alpine AS nodegui

WORKDIR /gui
COPY core/gui/package.json core/gui/yarn.lock ./
RUN corepack enable && corepack prepare yarn@4.5.1 --activate && yarn set version --yarn-path  4.5.1
# Fake git-version.js during yarn install to prevent git from causing cache
# invalidation of dependencies
RUN touch git-version.js && YARN_NODE_LINKER=node-modules yarn install

COPY core/gui .
# Position of .git doesn't matter since it's only there for the revision hash
COPY .git ./.git
RUN apk add --no-cache git && \
	node git-version.js && \
	apk del git && \
	yarn run build

FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.3_2.13.11

# copy all projects under core to /core
WORKDIR /core
COPY core/ .

RUN apt-get update
RUN apt-get install -y netcat unzip python3-pip
RUN pip3 install python-lsp-server python-lsp-server[websockets]
RUN pip3 install -r requirements.txt
RUN pip3 install -r operator-requirements.txt

WORKDIR /core
# Add .git for runtime calls to jgit from OPversion
COPY .git ../.git
COPY --from=nodegui /gui/dist ./gui/dist

RUN scripts/build-services.sh

CMD ["scripts/deploy-docker.sh"]

EXPOSE 8080

EXPOSE 9090

EXPOSE 8085