FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.3_2.13.11

# Set working directory
WORKDIR /core

# Copy all projects under core to /core
COPY core/ .

RUN rm -rf amber/user-resources/*

# Update system and install dependencies
RUN apt-get update && apt-get install -y \
    netcat \
    unzip \
    python3-pip \
    libpq-dev \
    python3-dev \
    && apt-get clean

# Install Python dependencies
RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install python-lsp-server python-lsp-server[websockets]

# Install requirements with a fallback for wordcloud
RUN pip3 install -r amber/requirements.txt
RUN pip3 install --no-cache-dir --find-links https://pypi.org/simple/ -r amber/operator-requirements.txt || \
    pip3 install --no-cache-dir wordcloud==1.9.2

# Additional setup
WORKDIR /core
# Add .git for runtime calls to jgit from OPversion
COPY .git ../.git

# Build services
RUN scripts/build-services.sh

# Set the default command
CMD ["scripts/workflow-computing-unit.sh"]

# Expose the required port
EXPOSE 8085