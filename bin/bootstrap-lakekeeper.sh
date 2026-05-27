#!/usr/bin/env bash
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

# Prerequisites:
#   - Already know how to setup Texera
#   - macOS or Linux (Lakekeeper does not publish Windows binaries)
#   - A running PostgreSQL instance (the same one Texera uses is fine)
#   - An accessible S3 bucket
#   - The AWS CLI (awscli) installed

set -e

# ==============================================================================
# User Configuration - Edit the values below before running this script
# ==============================================================================
#
# IMPORTANT: If you change values in storage.conf, you MUST also update the
# matching defaults here (or export the corresponding STORAGE_* environment
# variables before running this script)
#
# ==============================================================================

# Storage settings — must stay in sync with storage.conf
# if needed, update the default values after `:-` to match storage.conf
STORAGE_ICEBERG_CATALOG_REST_URI="${STORAGE_ICEBERG_CATALOG_REST_URI:-http://localhost:8181/catalog}"
STORAGE_ICEBERG_CATALOG_REST_WAREHOUSE_NAME="${STORAGE_ICEBERG_CATALOG_REST_WAREHOUSE_NAME:-texera}"
STORAGE_ICEBERG_CATALOG_REST_S3_BUCKET="${STORAGE_ICEBERG_CATALOG_REST_S3_BUCKET:-texera-iceberg}"
STORAGE_S3_REGION="${STORAGE_S3_REGION:-us-west-2}"
STORAGE_S3_ENDPOINT="${STORAGE_S3_ENDPOINT:-http://localhost:9000}"
STORAGE_S3_AUTH_USERNAME="${STORAGE_S3_AUTH_USERNAME:-texera_minio}"
STORAGE_S3_AUTH_PASSWORD="${STORAGE_S3_AUTH_PASSWORD:-password}"

# Lakekeeper binary — defaults to `lakekeeper` on $PATH (e.g. after
# `brew install lakekeeper`). Override by exporting LAKEKEEPER_BINARY_PATH
# or by editing the default below.
LAKEKEEPER_BINARY_PATH="${LAKEKEEPER_BINARY_PATH:-lakekeeper}"

# Lakekeeper PostgreSQL connection URLs
# LAKEKEEPER__PG_DATABASE_URL_READ="postgres://<user>:<urlencoded_password>@<host>:5432/texera_lakekeeper"
# LAKEKEEPER__PG_DATABASE_URL_WRITE="postgres://<user>:<urlencoded_password>@<host>:5432/texera_lakekeeper"
LAKEKEEPER__PG_DATABASE_URL_READ=""
LAKEKEEPER__PG_DATABASE_URL_WRITE=""

# Lakekeeper encryption key
LAKEKEEPER__PG_ENCRYPTION_KEY="texera_key"

# Lakekeeper metrics port
LAKEKEEPER__METRICS_PORT="9091"


# ==============================================================================
# End of User Configuration
# ==============================================================================

# Derive bootstrap-internal values from the storage settings above.
LAKEKEEPER_BASE_URI="${STORAGE_ICEBERG_CATALOG_REST_URI%/}"
LAKEKEEPER_BASE_URI="${LAKEKEEPER_BASE_URI%/catalog}"
WAREHOUSE_NAME="$STORAGE_ICEBERG_CATALOG_REST_WAREHOUSE_NAME"
S3_REGION="$STORAGE_S3_REGION"
S3_BUCKET="$STORAGE_ICEBERG_CATALOG_REST_S3_BUCKET"
S3_ENDPOINT="$STORAGE_S3_ENDPOINT"
S3_USERNAME="$STORAGE_S3_AUTH_USERNAME"
S3_PASSWORD="$STORAGE_S3_AUTH_PASSWORD"
STORAGE_PATH="s3://${S3_BUCKET}/iceberg/${WAREHOUSE_NAME}"

echo "=========================================="
echo "Lakekeeper Bootstrap and Warehouse Setup"
echo "=========================================="
echo "Lakekeeper Base URI: $LAKEKEEPER_BASE_URI"
echo "Lakekeeper Binary: $LAKEKEEPER_BINARY_PATH"
echo "Warehouse Name: $WAREHOUSE_NAME"
echo "S3 Endpoint: $S3_ENDPOINT"
echo "S3 Region: $S3_REGION"
echo "S3 Bucket: $S3_BUCKET"
echo "Storage Path: $STORAGE_PATH"
echo ""

# Function to check if Lakekeeper is running
check_lakekeeper_running() {
    local health_url="${LAKEKEEPER_BASE_URI}/health"
    if curl -s -f "$health_url" > /dev/null 2>&1; then
        return 0  # Running
    else
        return 1  # Not running
    fi
}

# Function to bootstrap the Lakekeeper server (creates default project).
# This is idempotent - safe to call even if already bootstrapped.
# Returns: 0=success (or already bootstrapped), 1=failure
bootstrap_lakekeeper_server() {
    local base_uri="$1"
    local bootstrap_url="${base_uri}/management/v1/bootstrap"

    echo "Bootstrapping Lakekeeper server (creating default project)..."
    echo "  URL: $bootstrap_url"

    local temp_response
    temp_response=$(mktemp) || {
        echo "✗ Failed to create temporary file"
        return 1
    }

    local http_code
    http_code=$(curl -s -o "$temp_response" -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d '{"accept-terms-of-use": true}' \
        "$bootstrap_url" 2>/dev/null || echo "000")

    echo "  HTTP status: $http_code"

    case "$http_code" in
        000)
            echo "✗ Failed to connect to Lakekeeper at $bootstrap_url"
            rm -f "$temp_response" || true
            return 1
            ;;
        2*)
            echo "✓ Lakekeeper server bootstrapped successfully (HTTP $http_code)"
            rm -f "$temp_response" || true
            return 0
            ;;
        *)
            if grep -q "CatalogAlreadyBootstrapped" "$temp_response" 2>/dev/null; then
                echo "✓ Lakekeeper server already bootstrapped (HTTP $http_code), continuing."
                rm -f "$temp_response" || true
                return 0
            fi
            echo "✗ Failed to bootstrap Lakekeeper server (HTTP $http_code)"
            echo "  Response body:"
            cat "$temp_response" | sed 's/^/    /' || true
            rm -f "$temp_response" || true
            return 1
            ;;
    esac
}

# Function to check if S3 bucket exists (requires AWS CLI)
check_S3_bucket() {
    local bucket_name="$1"
    local endpoint="$2"
    local username="$3"
    local password="$4"
    local region="$5"

    if ! command -v aws >/dev/null 2>&1; then
        echo "✗ Error: AWS CLI is required for S3 bucket operations."
        echo "  Install it with: pip install awscli"
        return 1
    fi

    if AWS_ACCESS_KEY_ID="$username" AWS_SECRET_ACCESS_KEY="$password" AWS_DEFAULT_REGION="$region" \
       aws --endpoint-url="$endpoint" s3 ls "s3://${bucket_name}/" >/dev/null 2>&1; then
        return 0  # Bucket exists
    else
        return 1  # Bucket doesn't exist or error
    fi
}

# Function to create S3 bucket (requires AWS CLI)
create_S3_bucket() {
    local bucket_name="$1"
    local endpoint="$2"
    local username="$3"
    local password="$4"
    local region="$5"

    if ! command -v aws >/dev/null 2>&1; then
        echo "✗ Error: AWS CLI is required for S3 bucket operations."
        echo "  Install it with: pip install awscli"
        return 1
    fi

    if AWS_ACCESS_KEY_ID="$username" AWS_SECRET_ACCESS_KEY="$password" AWS_DEFAULT_REGION="$region" \
       aws --endpoint-url="$endpoint" s3 mb "s3://${bucket_name}" >/dev/null 2>&1; then
        return 0  # Success
    else
        return 1  # Failed
    fi
}

# Function to start Lakekeeper
start_lakekeeper() {
    export LAKEKEEPER__METRICS_PORT
    export LAKEKEEPER__PG_ENCRYPTION_KEY

    echo "Starting Lakekeeper..."

    # Validate LAKEKEEPER_BINARY_PATH — `command -v` resolves both bare names
    # via $PATH lookup and absolute paths.
    if ! command -v "$LAKEKEEPER_BINARY_PATH" >/dev/null 2>&1; then
        echo "✗ Error: Lakekeeper binary '$LAKEKEEPER_BINARY_PATH' not found."
        echo "  Install it via 'brew install lakekeeper' (macOS) or set"
        echo "  LAKEKEEPER_BINARY_PATH to an absolute path / edit the default in this script."
        exit 1
    fi

    local binary_path="$LAKEKEEPER_BINARY_PATH"

    # Validate required database URLs
    if [ -z "$LAKEKEEPER__PG_DATABASE_URL_READ" ] || [ -z "$LAKEKEEPER__PG_DATABASE_URL_WRITE" ]; then
        echo "✗ Error: Database URLs not configured."
        echo "  Please set LAKEKEEPER__PG_DATABASE_URL_READ and LAKEKEEPER__PG_DATABASE_URL_WRITE"
        echo "  by exporting them as env vars or editing the User Configuration section in this script."
        exit 1
    fi
    export LAKEKEEPER__PG_DATABASE_URL_READ
    export LAKEKEEPER__PG_DATABASE_URL_WRITE

    # Run migration first
    echo "Running Lakekeeper migration..."
    if ! "$binary_path" migrate; then
        echo "✗ Failed to run Lakekeeper migration"
        return 1
    fi

    # Start Lakekeeper in background
    echo "Starting Lakekeeper server..."
    nohup "$binary_path" serve > /tmp/lakekeeper.log 2>&1 &
    local lakekeeper_pid=$!
    echo "Lakekeeper started with PID: $lakekeeper_pid"

    # Wait for Lakekeeper to be ready
    echo "Waiting for Lakekeeper to be ready..."
    local max_attempts=30
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        if check_lakekeeper_running; then
            echo "✓ Lakekeeper is ready!"
            return 0
        fi
        if [ $attempt -eq $max_attempts ]; then
            echo "✗ Lakekeeper did not become ready after $max_attempts attempts"
            echo "  Check logs at /tmp/lakekeeper.log"
            return 1
        fi
        echo "  Waiting for Lakekeeper... ($attempt/$max_attempts)"
        sleep 2
        attempt=$((attempt + 1))
    done
}

# Function to check if warehouse exists
# Returns: 0=exists, 1=not found, 2=connection error
check_warehouse_exists() {
    local warehouse_name="$1"
    local base_uri="$2"

    local list_url="${base_uri}/management/v1/warehouse"

    echo "Checking if warehouse '$warehouse_name' exists..."

    local temp_response
    temp_response=$(mktemp) || {
        echo "✗ Failed to create temporary file"
        return 2
    }

    local http_code
    http_code=$(curl -s -o "$temp_response" -w "%{http_code}" "$list_url" 2>/dev/null || echo "000")

    if [ "$http_code" = "000" ]; then
        rm -f "$temp_response" || true
        echo "✗ Failed to connect to Lakekeeper at $list_url"
        return 2
    fi

    if [ "$http_code" != "200" ]; then
        echo "⚠ Warning: Unexpected HTTP status $http_code when listing warehouses"
        cat "$temp_response" 2>/dev/null | sed 's/^/    /' || true
        rm -f "$temp_response" || true
        return 1
    fi

    # Check if warehouse name exists in the response
    # Response format: {"warehouses":[{"name":"...",...},...]}
    local found=1
    if command -v jq >/dev/null 2>&1; then
        if jq -e ".warehouses[] | select(.name == \"$warehouse_name\")" "$temp_response" >/dev/null 2>&1; then
            found=0
        fi
    else
        if grep -q "\"name\"[[:space:]]*:[[:space:]]*\"$warehouse_name\"" "$temp_response" 2>/dev/null; then
            found=0
        fi
    fi

    rm -f "$temp_response" 2>/dev/null || true
    return $found
}

# Function to create warehouse
# Args: warehouse_name base_uri s3_bucket s3_region s3_endpoint s3_username s3_password
# Returns: 0=success, 1=failure
create_warehouse() {
    local warehouse_name="$1"
    local base_uri="$2"
    local bucket="$3"
    local region="$4"
    local endpoint="$5"
    local username="$6"
    local password="$7"

    local create_url="${base_uri}/management/v1/warehouse"

    local create_payload=$(cat <<EOF
{
  "warehouse-name": "$warehouse_name",
  "storage-profile": {
    "type": "s3",
    "bucket": "$bucket",
    "region": "$region",
    "endpoint": "$endpoint",
    "flavor": "s3-compat",
    "path-style-access": true,
    "sts-enabled": false
  },
  "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "aws-access-key-id": "${username}",
      "aws-secret-access-key": "${password}"
    }
}
EOF
)

    echo "Creating warehouse '$warehouse_name'..."
    echo "  URL: $create_url"
    echo "  Bucket: $bucket, Region: $region, Endpoint: $endpoint"

    local temp_response
    temp_response=$(mktemp) || {
        echo "✗ Failed to create temporary file"
        return 1
    }

    local http_code
    http_code=$(curl -s -o "$temp_response" -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$create_payload" \
        "$create_url" || echo "000")

    echo "  HTTP status: $http_code"

    case "$http_code" in
        000)
            echo "✗ Failed to connect to Lakekeeper at $create_url"
            rm -f "$temp_response" || true
            return 1
            ;;
        2*)
            echo "✓ Warehouse '$warehouse_name' created successfully (HTTP $http_code)"
            rm -f "$temp_response" || true
            return 0
            ;;
        409)
            echo "✓ Warehouse '$warehouse_name' already exists (HTTP 409), treat as success."
            rm -f "$temp_response" || true
            return 0
            ;;
        *)
            echo "✗ Failed to create warehouse '$warehouse_name' (HTTP $http_code)"
            echo "  Response body:"
            cat "$temp_response" 2>/dev/null | sed 's/^/    /' || true
            rm -f "$temp_response" || true
            return 1
            ;;
    esac
}

# Step 1: Check if Lakekeeper is running, start if not
echo "Step 1: Checking Lakekeeper status..."
if check_lakekeeper_running; then
    echo "✓ Lakekeeper is already running"
else
    echo "Lakekeeper is not running, attempting to start..."
    if start_lakekeeper; then
        echo "✓ Lakekeeper started successfully"
    else
        echo "✗ Failed to start Lakekeeper"
        exit 1
    fi
fi
echo ""

# Step 2: Bootstrap the Lakekeeper server (creates default project)
echo "Step 2: Bootstrapping Lakekeeper server..."
if bootstrap_lakekeeper_server "$LAKEKEEPER_BASE_URI"; then
    echo "✓ Lakekeeper server bootstrap completed"
else
    echo "✗ Failed to bootstrap Lakekeeper server"
    echo "  Please check that Lakekeeper is running and accessible at $LAKEKEEPER_BASE_URI"
    exit 1
fi
echo ""

# Step 3: Check and create S3 bucket
echo "Step 3: Checking S3 bucket..."
if check_S3_bucket "$S3_BUCKET" "$S3_ENDPOINT" "$S3_USERNAME" "$S3_PASSWORD" "$S3_REGION"; then
    echo "✓ S3 bucket '$S3_BUCKET' already exists"
else
    echo "S3 bucket '$S3_BUCKET' does not exist, creating..."
    if create_S3_bucket "$S3_BUCKET" "$S3_ENDPOINT" "$S3_USERNAME" "$S3_PASSWORD" "$S3_REGION"; then
        echo "✓ S3 bucket '$S3_BUCKET' created successfully"
    else
        echo "✗ Failed to create S3 bucket '$S3_BUCKET'"
        echo "  Please ensure S3 is running and accessible at $S3_ENDPOINT"
        exit 1
    fi
fi
echo ""

# Step 4: Check and create warehouse
echo "Step 4: Checking and creating warehouse..."

set +e  # Temporarily disable exit on error to capture function return value
check_warehouse_exists "$WAREHOUSE_NAME" "$LAKEKEEPER_BASE_URI"
check_result=$?
set -e  # Re-enable exit on error

case $check_result in
    0)
        echo "✓ Warehouse '$WAREHOUSE_NAME' already exists, skipping creation."
        echo ""
        echo "=========================================="
        echo "✓ Bootstrap completed successfully!"
        echo "=========================================="
        exit 0
        ;;
    1)
        echo "Warehouse '$WAREHOUSE_NAME' does not exist, will create..."
        ;;
    2)
        exit 1
        ;;
    *)
        echo "✗ Unexpected error (code: $check_result)"
        exit 1
        ;;
esac

# Create warehouse
if create_warehouse "$WAREHOUSE_NAME" "$LAKEKEEPER_BASE_URI" "$S3_BUCKET" "$S3_REGION" "$S3_ENDPOINT" "$S3_USERNAME" "$S3_PASSWORD"; then
    echo ""
    echo "=========================================="
    echo "✓ Bootstrap completed successfully!"
    echo "=========================================="
    exit 0
else
    echo ""
    echo "=========================================="
    echo "✗ Bootstrap failed!"
    echo "=========================================="
    exit 1
fi
