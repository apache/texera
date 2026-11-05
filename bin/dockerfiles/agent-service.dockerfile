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

FROM docker.io/oven/bun:1.3.3-alpine@sha256:d2bc1fbc3afcd3d70afc2bb2544235bf559caae2a3084e9abed126e233797511

WORKDIR /app

COPY agent-service/package.json agent-service/bun.lock ./

# The download cache holds registry manifests fetched at build time; node_modules
# keeps its own copy of every file.
RUN bun install --frozen-lockfile --production \
 && rm -rf /root/.bun/install/cache

COPY agent-service/src ./src
COPY agent-service/tsconfig.json ./

COPY agent-service/LICENSE-binary ./LICENSE
COPY NOTICE ./NOTICE
COPY DISCLAIMER ./DISCLAIMER
COPY licenses ./licenses

# busybox adduser stamps the build day into /etc/shadow and ignores
# SOURCE_DATE_EPOCH; blank that field so the layer does not depend on the date.
RUN addgroup -S -g 1001 texera \
 && adduser -S -u 1001 -G texera -h /app texera \
 && sed -i 's/^texera:!:[0-9]*:/texera:!::/' /etc/shadow \
 && chown -R texera:texera /app
USER texera

EXPOSE 3001

CMD ["bun", "run", "src/server.ts"]
