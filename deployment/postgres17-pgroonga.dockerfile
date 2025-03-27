FROM docker.io/bitnami/postgresql:17.4.0-debian-12-r11

USER root

# Install prerequisites and add Groonga APT source
RUN apt-get update && apt-get install -y \
    ca-certificates \
    lsb-release \
    wget && \
    wget https://packages.groonga.org/debian/groonga-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt install -y ./groonga-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y \
    postgresql-17-pgdg-pgroonga \
    groonga-tokenizer-mecab && \
    rm -rf /var/lib/apt/lists/*

USER 1001  # go back to non-root user (bitnami/postgres default)

