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

# Bootstrap script to start Lakekeeper and create warehouse (idempotent).
# This script does three things:
#   1. Starts Lakekeeper if it's not already running
#   2. Checks if MinIO bucket exists (and creates it if needed)
#   3. Checks and creates the warehouse if it doesn't exist
#
#
# Usage:
#   ./bin/bootstrap-lakekeeper-warehouse.sh

set -e

# Read configuration from storage.conf or environment variables
# Priority: environment variable > storage.conf > default value

# Find storage.conf path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -n "$TEXERA_HOME" ]; then
    STORAGE_CONF_PATH="$TEXERA_HOME/common/config/src/main/resources/storage.conf"
else
    STORAGE_CONF_PATH="$SCRIPT_DIR/../common/config/src/main/resources/storage.conf"
fi

# Extract values from storage.conf using pyhocon for proper HOCON parsing
# pyhocon handles environment variable substitution correctly
if [ -f "$STORAGE_CONF_PATH" ]; then
    # Check if pyhocon is available
    if ! command -v python3 >/dev/null 2>&1; then
        echo "✗ Error: python3 is required to parse storage.conf"
        echo "  Please install Python 3"
        exit 1
    fi
    
    if ! python3 -c "import pyhocon" 2>/dev/null; then
        echo "✗ Error: pyhocon is required to parse storage.conf"
        echo "  Install it with: pip install pyhocon"
        exit 1
    fi
    
    # Use pyhocon for proper HOCON parsing (handles environment variable substitution)
    REST_URI_FROM_CONF=$(python3 "$SCRIPT_DIR/parse-storage-config.py" "storage.iceberg.catalog.rest.uri" 2>/dev/null | sed 's|/catalog/*$||' || echo "")
    WAREHOUSE_NAME_FROM_CONF=$(python3 "$SCRIPT_DIR/parse-storage-config.py" "storage.iceberg.catalog.rest.warehouse-name" 2>/dev/null || echo "")
    REST_REGION_FROM_CONF=$(python3 "$SCRIPT_DIR/parse-storage-config.py" "storage.iceberg.catalog.rest.region" 2>/dev/null || echo "")
    S3_BUCKET_FROM_CONF=$(python3 "$SCRIPT_DIR/parse-storage-config.py" "storage.iceberg.catalog.rest.s3-bucket" 2>/dev/null || echo "")
    S3_ENDPOINT_FROM_CONF=$(python3 "$SCRIPT_DIR/parse-storage-config.py" "storage.s3.endpoint" 2>/dev/null || echo "")
    S3_USERNAME_FROM_CONF=$(python3 "$SCRIPT_DIR/parse-storage-config.py" "storage.s3.auth.username" 2>/dev/null || echo "")
    S3_PASSWORD_FROM_CONF=$(python3 "$SCRIPT_DIR/parse-storage-config.py" "storage.s3.auth.password" 2>/dev/null || echo "")


    echo "Configuration read from storage.conf:"
    echo "  REST_URI_FROM_CONF=$REST_URI_FROM_CONF"
    echo "  WAREHOUSE_NAME_FROM_CONF=$WAREHOUSE_NAME_FROM_CONF"
    echo "  REST_REGION_FROM_CONF=$REST_REGION_FROM_CONF"
    echo "  S3_BUCKET_FROM_CONF=$S3_BUCKET_FROM_CONF"
    echo "  S3_ENDPOINT_FROM_CONF=$S3_ENDPOINT_FROM_CONF"
    echo "  S3_USERNAME_FROM_CONF=$S3_USERNAME_FROM_CONF"
    echo "  S3_PASSWORD_FROM_CONF=$S3_PASSWORD_FROM_CONF"
    echo ""
else
    REST_URI_FROM_CONF=""
    WAREHOUSE_NAME_FROM_CONF=""
    REST_REGION_FROM_CONF=""
    S3_BUCKET_FROM_CONF=""
    S3_ENDPOINT_FROM_CONF=""
    S3_USERNAME_FROM_CONF=""
    S3_PASSWORD_FROM_CONF=""
    echo "storage.conf not found, using environment variables or defaults"
    echo ""
fi

# Use values from storage.conf with defaults
LAKEKEEPER_BASE_URI="${REST_URI_FROM_CONF:-http://localhost:8181}"
WAREHOUSE_NAME="${WAREHOUSE_NAME_FROM_CONF:-texera-executions}"
S3_REGION="${REST_REGION_FROM_CONF:-us-west-2}"
S3_BUCKET="${S3_BUCKET_FROM_CONF:-texera-iceberg}"
S3_ENDPOINT="${S3_ENDPOINT_FROM_CONF:-http://localhost:9000}"
S3_USERNAME="${S3_USERNAME_FROM_CONF:-texera_minio}"
S3_PASSWORD="${S3_PASSWORD_FROM_CONF:-password}"
STORAGE_PATH="s3://${S3_BUCKET}/iceberg/${WAREHOUSE_NAME}"

echo "=========================================="
echo "Lakekeeper Bootstrap and Warehouse Setup"
echo "=========================================="
echo "Lakekeeper Base URI: $LAKEKEEPER_BASE_URI"
echo "Lakekeeper Binary: ${LAKEKEEPER_BINARY_PATH:-lakekeeper}"
echo "Warehouse Name: $WAREHOUSE_NAME"
echo "S3 Endpoint: $S3_ENDPOINT"
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

## Function to check if MinIO bucket exists
check_minio_bucket() {
    local bucket_name="$1"
    local endpoint="$2"
    local username="$3"
    local password="$4"

    # Use AWS CLI if available (preferred method)
    if command -v aws >/dev/null 2>&1; then
        # Check if bucket exists using AWS CLI (set env vars inline to avoid polluting global env)
        if AWS_ACCESS_KEY_ID="$username" AWS_SECRET_ACCESS_KEY="$password" AWS_DEFAULT_REGION="us-west-2" \
           aws --endpoint-url="$endpoint" s3 ls "s3://${bucket_name}/" >/dev/null 2>&1; then
            return 0  # Bucket exists
        else
            return 1  # Bucket doesn't exist or error
        fi
    else
        # Fallback: Use curl to check bucket via MinIO API
        # MinIO ListObjects API: GET /bucket-name?list-type=2
        local check_url="${endpoint}/${bucket_name}?list-type=2"
        local http_code=$(curl -s -o /dev/null -w "%{http_code}" \
            -u "${username}:${password}" \
            "$check_url" 2>/dev/null || echo "000")

        if [ "$http_code" = "200" ]; then
            return 0  # Bucket exists
        else
            return 1  # Bucket doesn't exist or error
        fi
    fi
}

# Function to create MinIO bucket
create_minio_bucket() {
    local bucket_name="$1"
    local endpoint="$2"
    local username="$3"
    local password="$4"

    # Use AWS CLI if available (preferred method)
    if command -v aws >/dev/null 2>&1; then
        # Create bucket using AWS CLI (set env vars inline to avoid polluting global env)
        if AWS_ACCESS_KEY_ID="$username" AWS_SECRET_ACCESS_KEY="$password" AWS_DEFAULT_REGION="us-west-2" \
           aws --endpoint-url="$endpoint" s3 mb "s3://${bucket_name}" >/dev/null 2>&1; then
            return 0  # Success
        else
            return 1  # Failed
        fi
    else
        # Fallback: Use curl to create bucket via MinIO API
        # MinIO MakeBucket API: PUT /bucket-name
        local create_url="${endpoint}/${bucket_name}"
        local http_code=$(curl -s -o /dev/null -w "%{http_code}" \
            -X PUT \
            -u "${username}:${password}" \
            "$create_url" 2>/dev/null || echo "000")

        if [ "$http_code" = "200" ]; then
            return 0  # Success
        else
            return 1  # Failed
        fi
    fi
}

# Function to start Lakekeeper
start_lakekeeper() {
    export LAKEKEEPER__METRICS_PORT=9091
#    export LAKEKEEPER__PG_DATABASE_URL_READ=
#    export LAKEKEEPER__PG_DATABASE_URL_WRITE=
#    export LAKEKEEPER__PG_ENCRYPTION_KEY=
#    local binary_path=""

    echo "Starting Lakekeeper..."

    # Check if LAKEKEEPER_BINARY_PATH is set
    if [ -z "${LAKEKEEPER_BINARY_PATH:-}" ]; then
        echo "⚠ Warning: LAKEKEEPER_BINARY_PATH environment variable is not set."
        echo "  Skipping Lakekeeper startup. Assuming it's already running or will be started separately."
        return 1
    fi

    # Check if the binary file exists and is executable
    if [ ! -x "$LAKEKEEPER_BINARY_PATH" ]; then
        echo "⚠ Warning: Lakekeeper binary not found or not executable at '$LAKEKEEPER_BINARY_PATH'"
        echo "  Please ensure LAKEKEEPER_BINARY_PATH points to a valid executable file."
        echo "  Skipping Lakekeeper startup. Assuming it's already running or will be started separately."
        return 1
    fi

    local binary_path="$LAKEKEEPER_BINARY_PATH"

    # Check required environment variables
    if [ -z "$LAKEKEEPER__PG_DATABASE_URL_READ" ] || [ -z "$LAKEKEEPER__PG_DATABASE_URL_WRITE" ] || [ -z "$LAKEKEEPER__PG_ENCRYPTION_KEY" ]; then
        echo "⚠ Warning: Required Lakekeeper database environment variables not set:"
        echo "  - LAKEKEEPER__PG_DATABASE_URL_READ"
        echo "  - LAKEKEEPER__PG_DATABASE_URL_WRITE"
        echo "  - LAKEKEEPER__PG_ENCRYPTION_KEY"
        echo "  Skipping Lakekeeper startup. Assuming it's already running or will be started separately."
        return 1
    fi

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
    
    # Get list of all warehouses and check if the name exists
    # API: GET /management/v1/warehouse returns list of warehouses
    local list_url="${base_uri}/management/v1/warehouse"
    
    echo "Checking if warehouse '$warehouse_name' exists..."
    echo "  URL: $list_url"
    
    # Get warehouse list
    local temp_response
    temp_response=$(mktemp) || {
        echo "✗ Failed to create temporary file"
        return 2
    }
    
    local http_code
    http_code=$(curl -s -o "$temp_response" -w "%{http_code}" "$list_url" 2>/dev/null || echo "000")
    echo "  HTTP status: $http_code"
    
    if [ "$http_code" = "000" ]; then
        rm -f "$temp_response" || true
        echo "✗ Failed to connect to Lakekeeper at $list_url"
        echo "  Please ensure Lakekeeper is running and accessible."
        return 2  # Connection error
    fi
    
    if [ "$http_code" != "200" ]; then
        echo "⚠ Warning: Unexpected HTTP status $http_code when listing warehouses"
        echo "  Response body:"
        cat "$temp_response" 2>/dev/null | sed 's/^/    /' || true
        rm -f "$temp_response" || true
        return 1  # Treat as not found, will attempt to create
    fi
    
    echo "  Checking response for warehouse name..."
    # Check if warehouse name exists in the list using jq or grep
    # The response format: {"warehouses":[{"name":"...",...},...]}
    if command -v jq >/dev/null 2>&1; then
        echo "  Using jq to parse response..."
        # Use jq if available (more reliable)
        if jq -e ".warehouses[] | select(.name == \"$warehouse_name\")" "$temp_response" >/dev/null 2>&1; then
            echo "  Warehouse found in list"
            rm -f "$temp_response" 2>/dev/null || true
            return 0  # Exists
        else
            echo "  Warehouse not found in list"
            rm -f "$temp_response" 2>/dev/null || true
            echo "  About to return 1 from check_warehouse_exists (jq path)"
            return 1  # Not found
        fi
    else
        echo "  Using grep to parse response (jq not available)..."
        # Fallback: use grep to check if name exists in JSON
        if grep -q "\"name\"[[:space:]]*:[[:space:]]*\"$warehouse_name\"" "$temp_response" 2>/dev/null; then
            echo "  Warehouse found in list"
            rm -f "$temp_response" || true
            return 0  # Exists
        else
            echo "  Warehouse not found in list"
            rm -f "$temp_response" 2>/dev/null || true
            echo "  About to return 1 from check_warehouse_exists (grep path)"
            return 1  # Not found
        fi
    fi
    echo "  Function check_warehouse_exists completed"
}

# Function to create warehouse
# Returns: 0=success, 1=failure
create_warehouse() {
    echo "1123"
    local warehouse_name="$1"
    local base_uri="$2"
    local storage_path="$3"
    local temp_response="$4"
    
    # NOTE: According to Lakekeeper 0.7.x Management API docs:
    # https://docs.lakekeeper.io/docs/0.7.x/api/management/#tag/warehouse
    # POST /management/v1/warehouse (singular) to create a warehouse
    # Request body uses "storage-profile" with "bucket" and "key-prefix" fields
    local create_url="${base_uri}/management/v1/warehouse"
    
    # Parse storage_path: s3://bucket/path -> bucket and key-prefix
    # Example: s3://texera-iceberg/iceberg/texera-executions
    #   -> bucket: texera-iceberg
    #   -> key-prefix: iceberg/texera-executions
    local bucket="${S3_BUCKET}"
    local region="${S3_REGION}"
    local endpoint="${S3_ENDPOINT}"
    
    # Request body format according to Lakekeeper API
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
      "aws-access-key-id": "${S3_USERNAME}",
      "aws-secret-access-key": "${S3_PASSWORD}"
    }
}
EOF
)
    
    echo "Creating warehouse '$warehouse_name'..."
    echo "  URL: $create_url"
    echo "  Request payload:"
    echo "$create_payload" | sed 's/^/    /'
    
    local http_code=$(curl -s -o "$temp_response" -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$create_payload" \
        "$create_url" || echo "000")
    
    echo "  HTTP status: $http_code"
    
    case "$http_code" in
        000)
            echo "✗ Failed to connect to Lakekeeper at $create_url"
            echo "  Please ensure Lakekeeper is running and accessible."
            return 1
            ;;
        2*)
            echo "✓ Warehouse '$warehouse_name' created successfully (HTTP $http_code)"
            return 0
            ;;
        409)
            echo "✓ Warehouse '$warehouse_name' already exists (HTTP 409), treat as success."
            return 0
            ;;
        *)
            echo "✗ Failed to create warehouse '$warehouse_name' (HTTP $http_code)"
            echo "Response body:"
            cat "$temp_response"
            echo ""
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
        echo "⚠ Could not start Lakekeeper automatically"
        echo "  Please start Lakekeeper manually or ensure it's accessible at $LAKEKEEPER_BASE_URI"
        echo "  Continuing with warehouse check/creation..."
    fi
fi
echo ""

# Step 2: Check and create MinIO bucket
echo "Step 3: Checking MinIO bucket..."
if check_minio_bucket "$S3_BUCKET" "$S3_ENDPOINT" "$S3_USERNAME" "$S3_PASSWORD"; then
    echo "✓ MinIO bucket '$S3_BUCKET' already exists"
else
    echo "MinIO bucket '$S3_BUCKET' does not exist, creating..."
    if create_minio_bucket "$S3_BUCKET" "$S3_ENDPOINT" "$S3_USERNAME" "$S3_PASSWORD"; then
        echo "✓ MinIO bucket '$S3_BUCKET' created successfully"
    else
        echo "✗ Failed to create MinIO bucket '$S3_BUCKET'"
        echo "  Please ensure MinIO is running and accessible at $S3_ENDPOINT"
        echo "  Check credentials: username=$S3_USERNAME"
        exit 1
    fi
fi
echo ""

# Step 3: Check and create warehouse
echo "Step 3: Checking and creating warehouse..."

# Create temporary file for response (cleanup on exit)
TEMP_RESPONSE=$(mktemp)
trap "rm -f $TEMP_RESPONSE" EXIT

# Check if warehouse exists
echo "Calling check_warehouse_exists..."
set +e  # Temporarily disable exit on error to capture function return value
check_warehouse_exists "$WAREHOUSE_NAME" "$LAKEKEEPER_BASE_URI"
check_result=$?
set -e  # Re-enable exit on error
echo "check_warehouse_exists returned: $check_result"
echo "Entering case statement with check_result=$check_result"

case $check_result in
    0)
        echo "Case 0: Warehouse exists"
        echo "✓ Warehouse '$WAREHOUSE_NAME' already exists, skipping creation."
        echo ""
        echo "=========================================="
        echo "✓ Bootstrap completed successfully!"
        echo "=========================================="
        exit 0
        ;;
    1)
        echo "Case 1: Warehouse not found, will create"
        echo "Warehouse '$WAREHOUSE_NAME' does not exist, will create..."
        ;;
    2)
        echo "Case 2: Connection error"
        exit 1
        ;;
    *)
        echo "Case *: Unexpected return value: $check_result"
        exit 1
        ;;
esac

echo "After case statement, about to call create_warehouse..."

# Create warehouse
if create_warehouse "$WAREHOUSE_NAME" "$LAKEKEEPER_BASE_URI" "$STORAGE_PATH" "$TEMP_RESPONSE"; then
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

