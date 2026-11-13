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

FROM docker.io/library/node:24.19.0-slim@sha256:a9f5f7c91a432850b2a8a7797adf5eadb6c733ceed61167806cee7ea7fbc29df

WORKDIR /usr/src/app

COPY package.json package-lock.json ./

# npm's download cache indexes entries by fetch time, its debug log is named
# after the clock, and Node's compile cache in /tmp varies run to run;
# node_modules keeps its own copy of every file.
RUN npm ci \
 && rm -rf /root/.npm /tmp/node-compile-cache

# Define environment variable
ENV YPERSISTENCE=./leveldb
ENV HOST=0.0.0.0

# Run y-websocket server when the container launches
CMD ["npx", "y-websocket"]

# Make port 1234 available to the world outside this container
EXPOSE 1234