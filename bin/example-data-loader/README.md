<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Texera Example Data Loader

Loads example datasets and workflows into Texera using a lightweight shell script.

## Usage

```bash
# Basic usage
./load-examples.sh

# With custom URL and credentials
./load-examples.sh --url http://texera:8080 --username admin --password admin

# Using environment variables
export TEXERA_URL=http://texera:8080
export TEXERA_USERNAME=admin
export TEXERA_PASSWORD=admin
./load-examples.sh

# Show help
./load-examples.sh --help
```

## Environment Variables

| Variable | Default |
|----------|---------|
| `TEXERA_URL` | `http://localhost:8080` |
| `TEXERA_FILE_SERVICE_URL` | `http://localhost:8080` |
| `TEXERA_USERNAME` | `admin` |
| `TEXERA_PASSWORD` | `admin` |
| `SKIP_DATASETS` | `false` |
| `SKIP_WORKFLOWS` | `false` |
| `VERBOSE` | `false` |

## Build Docker Image

```bash
# From project root
docker build -f bin/texera-example-data-loader.dockerfile -t texera/texera-example-data-loader .

# Run
docker run --rm \
  -e TEXERA_URL=http://host.docker.internal:8080 \
  -e TEXERA_USERNAME=admin \
  -e TEXERA_PASSWORD=admin \
  texera/texera-example-data-loader
```
