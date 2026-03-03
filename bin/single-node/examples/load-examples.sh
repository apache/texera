#!/bin/bash

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

# Unified script to load example datasets and workflows into Texera.
# This script is designed to run inside a Docker container as a one-shot job.

set -euo pipefail

# Configuration from environment variables with defaults
TEXERA_WEB_APPLICATION_URL=${TEXERA_WEB_APPLICATION_URL:-"http://texera-web-application:8080/api"}
TEXERA_FILE_SERVICE_URL=${TEXERA_FILE_SERVICE_URL:-"http://texera-file-service:9092/api"}
USERNAME=${TEXERA_EXAMPLE_USERNAME:-"texera"}
PASSWORD=${TEXERA_EXAMPLE_PASSWORD:-"texera"}
# In Texera, registration sets email = username
OWNER_EMAIL="$USERNAME"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATASET_DIR="$SCRIPT_DIR/datasets"
WORKFLOW_DIR="$SCRIPT_DIR/workflows"

MAX_RETRIES=60
RETRY_INTERVAL=5

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Wait for a service to become healthy
wait_for_service() {
    local service_name="$1"
    local health_url="$2"
    local retries=0

    print_status "Waiting for $service_name to be ready..."
    while [ $retries -lt $MAX_RETRIES ]; do
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$health_url" 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" = "200" ]; then
            print_status "$service_name is healthy!"
            return 0
        fi
        retries=$((retries + 1))
        print_status "Waiting for $service_name... (attempt $retries/$MAX_RETRIES, status: $HTTP_CODE)"
        sleep $RETRY_INTERVAL
    done

    print_error "$service_name did not become healthy after $MAX_RETRIES attempts"
    return 1
}

# Authenticate and obtain JWT token
authenticate() {
    print_status "Logging in as $USERNAME..."
    LOGIN_RESPONSE=$(curl -s -X POST "$TEXERA_WEB_APPLICATION_URL/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")

    if echo "$LOGIN_RESPONSE" | grep -q '"accessToken"'; then
        TOKEN=$(echo "$LOGIN_RESPONSE" | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)
        print_status "Login successful"
        return 0
    fi

    print_status "User doesn't exist, attempting to register..."
    REGISTER_RESPONSE=$(curl -s -X POST "$TEXERA_WEB_APPLICATION_URL/auth/register" \
        -H "Content-Type: application/json" \
        -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")

    if echo "$REGISTER_RESPONSE" | grep -q '"accessToken"'; then
        TOKEN=$(echo "$REGISTER_RESPONSE" | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)
        print_status "Registration successful"
        return 0
    fi

    print_error "Authentication failed"
    return 1
}

# Load all datasets from the datasets directory
load_datasets() {
    if [ ! -d "$DATASET_DIR" ]; then
        print_warn "Dataset directory '$DATASET_DIR' not found, skipping datasets"
        return 0
    fi

    # Get list of existing datasets
    LIST_RESPONSE=$(curl -s -X GET "$TEXERA_FILE_SERVICE_URL/dataset/list" \
        -H "Authorization: Bearer $TOKEN")

    for dataset_folder in "$DATASET_DIR"/*/; do
        [ -d "$dataset_folder" ] || continue

        DATASET_NAME=$(basename "$dataset_folder")
        print_status "Processing dataset: $DATASET_NAME"

        # Check if dataset already exists
        if echo "$LIST_RESPONSE" | grep -q "\"name\":\"$DATASET_NAME\""; then
            print_status "Dataset '$DATASET_NAME' already exists, skipping"
            continue
        fi

        # Read description.txt if available
        DATASET_DESCRIPTION=""
        if [ -f "$dataset_folder/description.txt" ]; then
            DATASET_DESCRIPTION=$(<"$dataset_folder/description.txt")
        fi

        # Create dataset
        CREATE_RESPONSE=$(curl -s -X POST "$TEXERA_FILE_SERVICE_URL/dataset/create" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TOKEN" \
            -d "{
                \"datasetName\": \"$DATASET_NAME\",
                \"datasetDescription\": \"$DATASET_DESCRIPTION\",
                \"isDatasetPublic\": true
            }")

        DATASET_ID=$(echo "$CREATE_RESPONSE" | grep -o '"did":[0-9]*' | cut -d':' -f2)

        if [ -z "$DATASET_ID" ]; then
            print_error "Failed to create dataset '$DATASET_NAME'"
            continue
        fi

        print_status "Created dataset '$DATASET_NAME' with ID $DATASET_ID"

        # Upload files using the multipart upload API (avoids presigned URL issues in Docker)
        for file in "$dataset_folder"*; do
            [ -f "$file" ] || continue
            FILENAME=$(basename "$file")
            [ "$FILENAME" = "description.txt" ] && continue

            print_status "Uploading file: $FILENAME"
            ENCODED_NAME=$(echo "$FILENAME" | sed 's/ /%20/g')
            FILE_SIZE=$(wc -c < "$file" | tr -d ' ')

            # Step 1: Initialize multipart upload (single part for small files)
            INIT_RESPONSE=$(curl -s -X POST \
                "$TEXERA_FILE_SERVICE_URL/dataset/multipart-upload?type=init&ownerEmail=$OWNER_EMAIL&datasetName=$DATASET_NAME&filePath=$ENCODED_NAME&fileSizeBytes=$FILE_SIZE&partSizeBytes=$FILE_SIZE" \
                -H "Authorization: Bearer $TOKEN" \
                -H "Content-Type: application/json")

            if ! echo "$INIT_RESPONSE" | grep -q '"missingParts"'; then
                print_error "Failed to init upload for $FILENAME: $INIT_RESPONSE"
                continue
            fi

            # Step 2: Upload the single part
            HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
                "$TEXERA_FILE_SERVICE_URL/dataset/multipart-upload/part?ownerEmail=$OWNER_EMAIL&datasetName=$DATASET_NAME&filePath=$ENCODED_NAME&partNumber=1" \
                -H "Authorization: Bearer $TOKEN" \
                -H "Content-Type: application/octet-stream" \
                -H "Content-Length: $FILE_SIZE" \
                --data-binary "@$file")

            if [ "$HTTP_CODE" != "200" ]; then
                print_error "Failed to upload part for $FILENAME (HTTP $HTTP_CODE)"
                # Abort the upload session
                curl -s -o /dev/null -X POST \
                    "$TEXERA_FILE_SERVICE_URL/dataset/multipart-upload?type=abort&ownerEmail=$OWNER_EMAIL&datasetName=$DATASET_NAME&filePath=$ENCODED_NAME" \
                    -H "Authorization: Bearer $TOKEN" \
                    -H "Content-Type: application/json"
                continue
            fi

            # Step 3: Finish the multipart upload
            FINISH_RESPONSE=$(curl -s -X POST \
                "$TEXERA_FILE_SERVICE_URL/dataset/multipart-upload?type=finish&ownerEmail=$OWNER_EMAIL&datasetName=$DATASET_NAME&filePath=$ENCODED_NAME" \
                -H "Authorization: Bearer $TOKEN" \
                -H "Content-Type: application/json")

            if echo "$FINISH_RESPONSE" | grep -q '"message"'; then
                print_status "Uploaded $FILENAME"
            else
                print_error "Failed to finish upload for $FILENAME: $FINISH_RESPONSE"
            fi
        done

        # Create version
        print_status "Creating version for $DATASET_NAME"
        VERSION_RESPONSE=$(curl -s -X POST "$TEXERA_FILE_SERVICE_URL/dataset/$DATASET_ID/version/create" \
            -H "Content-Type: text/plain" \
            -H "Authorization: Bearer $TOKEN" \
            -d "")

        if echo "$VERSION_RESPONSE" | grep -q '"datasetVersion"'; then
            print_status "Version created successfully for $DATASET_NAME"
        else
            print_error "Failed to create version for $DATASET_NAME"
        fi
    done

    print_status "All datasets processed"
}

# Load all workflows from the workflows directory
load_workflows() {
    if [ ! -d "$WORKFLOW_DIR" ]; then
        print_warn "Workflow directory '$WORKFLOW_DIR' not found, skipping workflows"
        return 0
    fi

    # Get list of existing workflows
    WORKFLOW_LIST_RESPONSE=$(curl -s -X GET "$TEXERA_WEB_APPLICATION_URL/workflow/list" \
        -H "Authorization: Bearer $TOKEN")

    for workflow_file in "$WORKFLOW_DIR"/*.json; do
        [ -f "$workflow_file" ] || continue

        workflow_name=$(basename "$workflow_file" .json)
        print_status "Processing workflow: $workflow_name"

        # Check if workflow already exists
        if echo "$WORKFLOW_LIST_RESPONSE" | grep -q "\"name\":\"$workflow_name\""; then
            print_status "Workflow '$workflow_name' already exists, skipping"
            continue
        fi

        # Parse and create workflow
        content=$(jq -c . "$workflow_file")
        if [ $? -ne 0 ]; then
            print_error "Failed to parse $workflow_file with jq"
            continue
        fi

        print_status "Creating workflow: $workflow_name"
        response=$(curl -s -X POST "$TEXERA_WEB_APPLICATION_URL/workflow/create" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            -d "{\"name\":\"$workflow_name\", \"content\": $(jq -Rs <<< "$content")}")

        if echo "$response" | grep -q '"wid"'; then
            wid=$(echo "$response" | grep -o '"wid":[0-9]*' | cut -d':' -f2)
            print_status "Workflow '$workflow_name' created with ID $wid"
        else
            print_error "Failed to create workflow '$workflow_name'"
            print_error "Response: $response"
        fi
    done

    print_status "All workflows processed"
}

# Main execution
main() {
    print_status "=== Texera Example Data Loader ==="

    wait_for_service "Web Application" "$TEXERA_WEB_APPLICATION_URL/healthcheck"
    wait_for_service "File Service" "$TEXERA_FILE_SERVICE_URL/healthcheck"

    authenticate

    load_datasets
    load_workflows

    print_status "=== Example data loading complete ==="
}

main
