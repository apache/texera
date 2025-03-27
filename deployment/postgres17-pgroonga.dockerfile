FROM bitnami/postgresql:17.4.0-debian-12-r11

USER root

# Install build dependencies
RUN install_packages \
    build-essential \
    git \
    wget \
    curl \
    ca-certificates \
    pkg-config \
    libgroonga-dev \
    libmecab-dev \
    mecab \
    groonga-tokenizer-mecab \
    postgresql-server-dev-all \
    gnupg

# Clone and build PGroonga
RUN git clone --depth 1 https://github.com/pgroonga/pgroonga.git /tmp/pgroonga && \
    cd /tmp/pgroonga && \
    PG_CONFIG=/opt/bitnami/postgresql/bin/pg_config make && \
    PG_CONFIG=/opt/bitnami/postgresql/bin/pg_config make install && \
    rm -rf /tmp/pgroonga

USER 1001
