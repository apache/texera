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

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY bin/examples/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy example loader scripts
COPY bin/examples/login.py .
COPY bin/examples/load_dataset.py .
COPY bin/examples/load_workflow.py .
COPY bin/examples/main.py .

# Copy example data
COPY bin/examples/dataset ./dataset
COPY bin/examples/workflow ./workflow

# Make main.py executable
RUN chmod +x main.py

# Set environment variables with defaults
ENV TEXERA_URL=http://localhost:8080
ENV TEXERA_USERNAME=admin
ENV TEXERA_PASSWORD=admin
ENV TEXERA_EMAIL=admin

# Run the loader
ENTRYPOINT ["python", "main.py"]

# Default arguments (can be overridden)
CMD []
