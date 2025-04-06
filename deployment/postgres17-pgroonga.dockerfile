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
