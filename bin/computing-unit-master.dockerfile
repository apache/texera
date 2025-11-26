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
COPY amber/ amber/
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

RUN sbt clean WorkflowExecutionService/dist

# Unzip the texera binary
RUN unzip amber/target/universal/amber-*.zip -d amber/target/

FROM eclipse-temurin:11-jdk-jammy AS runtime

# Build argument to enable/disable R support (default: true for backward compatibility)
ARG WITH_R_SUPPORT=false

WORKDIR /texera/amber

COPY --from=build /texera/amber/r-requirements.txt /tmp/r-requirements.txt
COPY --from=build /texera/amber/requirements.txt /tmp/requirements.txt
COPY --from=build /texera/amber/operator-requirements.txt /tmp/operator-requirements.txt

# Install Python runtime dependencies (always) and R runtime dependencies (conditional)
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    libpq-dev \
    curl \
    unzip \
    $(if [ "$WITH_R_SUPPORT" = "true" ]; then echo "\
    gfortran \
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
    libcurl4-openssl-dev"; fi) \
    && apt-get clean

# Install R and needed libraries (conditional)
ENV R_VERSION=4.3.3
RUN if [ "$WITH_R_SUPPORT" = "true" ]; then \
        curl -O https://cran.r-project.org/src/base/R-4/R-${R_VERSION}.tar.gz && \
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
        pip3 install -r /tmp/r-requirements.txt; \
    else \
        pip3 install --upgrade pip setuptools wheel && \
        pip3 install -r /tmp/requirements.txt && \
        pip3 install -r /tmp/operator-requirements.txt; \
    fi
# Install R packages, pinning arrow to 14.0.2.1 explicitly (conditional)
RUN if [ "$WITH_R_SUPPORT" = "true" ]; then \
        Rscript -e "options(repos = c(CRAN = 'https://cran.r-project.org')); \
                    install.packages(c('coro', 'dplyr'), \
                                     Ncpus = parallel::detectCores())" && \
        Rscript -e "options(repos = c(CRAN = 'https://cran.r-project.org')); \
                    if (!requireNamespace('remotes', quietly=TRUE)) \
                      install.packages('remotes'); \
                    remotes::install_version('arrow', version='14.0.2.1', \
                      repos='https://cran.r-project.org', upgrade='never'); \
                    cat('R arrow version: ', as.character(packageVersion('arrow')), '\n')"; \
    fi
ENV LD_LIBRARY_PATH=/usr/local/lib/R/lib:$LD_LIBRARY_PATH

# Copy the built texera binary from the build phase
COPY --from=build /texera/.git /texera/amber/.git
COPY --from=build /texera/amber/target/amber-* /texera/amber/
# Copy resources directories from build phase
COPY --from=build /texera/common/config/src/main/resources /texera/amber/common/config/src/main/resources
COPY --from=build /texera/amber/src/main/resources /texera/amber/src/main/resources
# Copy code for python & R UDF
COPY --from=build /texera/amber/src/main/python /texera/amber/src/main/python

CMD ["bin/computing-unit-master"]

EXPOSE 8085
