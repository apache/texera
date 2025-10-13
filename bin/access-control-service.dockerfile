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
WORKDIR /texera

# Copy modules for building the service
COPY common/ common/
COPY access-control-service/ access-control-service/
COPY project/ project/
COPY build.sbt build.sbt

# Update system and install dependencies
RUN apt-get update && apt-get install -y \
    netcat \
    unzip \
    libpq-dev \
    && apt-get clean

# Add .git for runtime calls to jgit from OPversion
COPY .git .git

RUN sbt clean AccessControlService/dist

# Unzip the texera binary
RUN unzip access-control-service/target/universal/access-control-service-*.zip -d target/

FROM eclipse-temurin:11-jre-jammy AS runtime

WORKDIR /texera

COPY --from=build /texera/.git /texera/.git
# Copy the built texera binary from the build phase
COPY --from=build /texera/target/access-control-service* /texera/
# Copy resources directories from build phase
COPY --from=build /texera/access-control-service/src/main/resources /texera/access-control-service/src/main/resources

CMD ["bin/access-control-service"]

EXPOSE 9096