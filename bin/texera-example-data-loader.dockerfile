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

# Use alpine for minimal image size (~10MB vs ~200MB with Python)
FROM alpine:latest

# Install curl, bash, and jq
RUN apk add --no-cache curl bash jq

# Set working directory
WORKDIR /app

# Copy the shell script
COPY bin/example-data-loader/load-examples.sh /app/

# Copy example data
COPY bin/example-data-loader/dataset ./dataset/
COPY bin/example-data-loader/workflow ./workflow/

# Make script executable
RUN chmod +x /app/load-examples.sh

# Set environment variables with defaults
ENV TEXERA_URL=http://localhost:8080
ENV TEXERA_FILE_SERVICE_URL=http://localhost:8080
ENV TEXERA_USERNAME=admin
ENV TEXERA_PASSWORD=admin
ENV DATASET_DIR=/app/dataset
ENV WORKFLOW_DIR=/app/workflow
ENV SKIP_DATASETS=false
ENV SKIP_WORKFLOWS=false
ENV SKIP_SERVICE_CHECK=false
ENV VERBOSE=false

# Run the loader script
ENTRYPOINT ["/app/load-examples.sh"]

# Default arguments (can be overridden)
CMD []
