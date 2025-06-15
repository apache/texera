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
RUN unzip amber/target/universal/texera-0.1-SNAPSHOT.zip -d amber/target/

FROM eclipse-temurin:11-jre-jammy AS runtime

WORKDIR /core/amber

COPY --from=build /core/amber/r-requirements.txt /tmp/r-requirements.txt
COPY --from=build /core/amber/requirements.txt /tmp/requirements.txt
COPY --from=build /core/amber/operator-requirements.txt /tmp/operator-requirements.txt

# Install Python & R runtime dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    libpq-dev \
    gfortran \
    curl \
    build-essential \
    libreadline-dev \
    libncurses-dev \
    libssl-dev \
    libxml2-dev \
    xorg-dev \
    libbz2-dev \
    liblzma-dev \
    libpcre++-dev \
    libpango1.0-dev \
     libcurl4-openssl-dev \
    unzip \
    && apt-get clean

# Install R and needed libraries
ENV R_VERSION=4.3.3
RUN curl -O https://cran.r-project.org/src/base/R-4/R-${R_VERSION}.tar.gz && \
    tar -xf R-${R_VERSION}.tar.gz && \
    cd R-${R_VERSION} && \
    ./configure --prefix=/usr/local \
                --enable-R-shlib \
                --with-blas \
                --with-lapack && \
    make -j 4 && \
    make install && \
    cd .. && \
    rm -rf R-${R_VERSION}* && R --version && pip3 install --upgrade pip setuptools wheel && \
    pip3 install -r /tmp/requirements.txt && \
    pip3 install -r /tmp/operator-requirements.txt && \
    pip3 install -r /tmp/r-requirements.txt
RUN Rscript -e "options(repos = c(CRAN = 'https://cran.r-project.org')); \
                install.packages(c('coro', 'arrow', 'dplyr'), \
                                 Ncpus = parallel::detectCores())"
ENV LD_LIBRARY_PATH=/usr/local/lib/R/lib:$LD_LIBRARY_PATH

# Copy the built texera binary from the build phase
COPY --from=build /.git /.git
COPY --from=build /core/amber/target/texera-0.1-SNAPSHOT /core/amber
# Copy resources directories under /core from build phase
COPY --from=build /core/config/src/main/resources /core/config/src/main/resources
COPY --from=build /core/amber/src/main/resources /core/amber/src/main/resources
# Copy code for python & R UDF
COPY --from=build /core/amber/src/main/python /core/amber/src/main/python

CMD ["bin/computing-unit-master"]

EXPOSE 8085
