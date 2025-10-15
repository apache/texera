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

FROM bitnami/postgresql:17.4.0-debian-12-r11

USER root

# Install build tools and Groonga APT repo
RUN install_packages \
    build-essential \
    git \
    wget \
    curl \
    ca-certificates \
    pkg-config \
    libmecab-dev \
    mecab \
    gnupg \
    libpq-dev

# Add Groonga official APT repo
RUN wget https://packages.groonga.org/debian/groonga-apt-source-latest-bookworm.deb && \
    dpkg -i groonga-apt-source-latest-bookworm.deb && \
    apt-get update && \
    apt-get install -y \
    libgroonga-dev \
    groonga-tokenizer-mecab

# Clone PGroonga with submodules and build it using Bitnami's pg_config
RUN git clone --recursive https://github.com/pgroonga/pgroonga.git /tmp/pgroonga && \
    cd /tmp/pgroonga && \
    PG_CONFIG=/opt/bitnami/postgresql/bin/pg_config make && \
    PG_CONFIG=/opt/bitnami/postgresql/bin/pg_config make install && \
    rm -rf /tmp/pgroonga

USER 1001
