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

RUN chmod 1777 /tmp
RUN apt-get update
RUN apt-get install -y netcat unzip python3-pip gfortran libreadline-dev libx11-dev libxt-dev libxext-dev libxmu-dev libxrender-dev libbz2-dev liblzma-dev libpcre2-dev libcurl4-openssl-dev libpng-dev libxml2-dev libcairo2-dev libfontconfig1-dev libfreetype6-dev libharfbuzz-dev libfribidi-dev libtiff5-dev libssl-dev libpq-dev && apt-get clean && rm -rf /var/lib/apt/lists/* 

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget software-properties-common && \
    wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.0-1_all.deb && \
    dpkg -i cuda-keyring_1.0-1_all.deb && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    cuda-toolkit-12-1 && \
    rm -rf /var/lib/apt/lists/*

ENV CUDA_HOME=/usr/local/cuda
ENV PATH="$CUDA_HOME/bin:$PATH"
ENV LD_LIBRARY_PATH="$CUDA_HOME/lib64:$LD_LIBRARY_PATH"

RUN pip3 install python-lsp-server python-lsp-server[websockets]

RUN python3 --version && nvcc --version

# Download and install R 4.3.3
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
    rm -rf R-${R_VERSION}* && R --version && pip3 install --upgrade pip setuptools && \
    pip3 install -r amber/requirements.txt && \
    pip3 install -r amber/operator-requirements.txt && \
    pip3 install -r amber/r-requirements.txt

RUN pip3 install flash-attn --no-build-isolation
RUN pip3 install esm-efficient
RUN pip3 install gReLU==1.0.4.post1.dev0

# Set CRAN repository and install all necessary CRAN packages in parallel
RUN Rscript -e "options(repos = c(CRAN = 'https://cran.r-project.org')); \
                install.packages(c('Seurat', 'BiocManager', 'R.utils', 'ggplotify', 'harmony', 'bench', 'coro', 'reticulate', 'arrow', 'scSorter', 'igraph', 'leiden'), \
                                 Ncpus = parallel::detectCores())"

# Install Bioconductor packages in parallel
RUN Rscript -e "BiocManager::install(c('SingleCellExperiment', 'scDblFinder', 'glmGamPoi'), \
                                     Ncpus = parallel::detectCores())"

WORKDIR /core
# Add .git for runtime calls to jgit from OPversion
COPY .git ../.git
COPY --from=nodegui /gui/dist ./gui/dist

RUN scripts/build-services.sh

CMD ["scripts/deploy-docker.sh"]

EXPOSE 8080

EXPOSE 9090

EXPOSE 8085
