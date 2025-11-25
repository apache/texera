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

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

TEXERA_URL="${TEXERA_URL:-http://localhost:8080}"
TEXERA_FILE_SERVICE_URL="${TEXERA_FILE_SERVICE_URL:-http://localhost:8080}"
TEXERA_USERNAME="${TEXERA_USERNAME:-texera}"
TEXERA_PASSWORD="${TEXERA_PASSWORD:-texera}"
DATASET_DIR="${DATASET_DIR:-$(dirname "$0")/dataset}"
WORKFLOW_DIR="${WORKFLOW_DIR:-$(dirname "$0")/workflow}"
SKIP_DATASETS="${SKIP_DATASETS:-false}"
SKIP_WORKFLOWS="${SKIP_WORKFLOWS:-false}"
SKIP_SERVICE_CHECK="${SKIP_SERVICE_CHECK:-false}"
VERBOSE="${VERBOSE:-false}"

JWT_TOKEN=""
AUTH_HEADER=""

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_debug() { [ "$VERBOSE" = "true" ] && echo -e "[DEBUG] $1"; }

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Load example datasets and workflows into Texera.

Options:
    --url URL               Texera server URL (default: http://localhost:8080)
    --file-service-url URL  File service URL (default: http://localhost:8080)
    --username USERNAME     Username (default: texera)
    --password PASSWORD     Password (default: texera)
    --dataset-dir DIR       Dataset directory (default: ./dataset)
    --workflow-dir DIR      Workflow directory (default: ./workflow)
    --skip-datasets         Skip loading datasets
    --skip-workflows        Skip loading workflows
    --skip-service-check    Skip service availability check
    --verbose, -v           Enable verbose logging
    --help, -h              Show this help message
EOF
}

while [ $# -gt 0 ]; do
    case $1 in
        --url) TEXERA_URL="$2"; shift 2 ;;
        --file-service-url) TEXERA_FILE_SERVICE_URL="$2"; shift 2 ;;
        --username) TEXERA_USERNAME="$2"; shift 2 ;;
        --password) TEXERA_PASSWORD="$2"; shift 2 ;;
        --dataset-dir) DATASET_DIR="$2"; shift 2 ;;
        --workflow-dir) WORKFLOW_DIR="$2"; shift 2 ;;
        --skip-datasets) SKIP_DATASETS="true"; shift ;;
        --skip-workflows) SKIP_WORKFLOWS="true"; shift ;;
        --skip-service-check) SKIP_SERVICE_CHECK="true"; shift ;;
        --verbose|-v) VERBOSE="true"; shift ;;
        --help|-h) show_usage; exit 0 ;;
        *) log_error "Unknown option: $1"; show_usage; exit 1 ;;
    esac
done

TEXERA_URL="${TEXERA_URL%/}"
TEXERA_FILE_SERVICE_URL="${TEXERA_FILE_SERVICE_URL%/}"

json_value() {
    local json="$1" key="$2"
    if command -v jq >/dev/null 2>&1; then
        echo "$json" | jq -r ".$key // empty"
    else
        echo "$json" | grep -o "\"$key\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" | sed "s/.*\"\([^\"]*\)\"$/\1/"
    fi
}

is_success() {
    case "$1" in
        2[0-9][0-9]) return 0 ;;
        *) return 1 ;;
    esac
}

http_request() {
    local method="$1" url="$2" data="$3" content_type="${4:-application/json}"
    if [ -n "$data" ]; then
        curl -s -w "\n%{http_code}" --connect-timeout 10 --max-time 60 -X "$method" \
            -H "$AUTH_HEADER" -H "Content-Type: $content_type" -d "$data" "$url" 2>/dev/null
    else
        curl -s -w "\n%{http_code}" --connect-timeout 10 --max-time 60 -X "$method" \
            -H "$AUTH_HEADER" "$url" 2>/dev/null
    fi
}

parse_response() {
    local response="$1"
    HTTP_CODE=$(echo "$response" | tail -n1)
    HTTP_BODY=$(echo "$response" | sed '$d')
}

authenticate() {
    log_info "Authenticating with Texera server..."
    local response
    response=$(curl -s -w "\n%{http_code}" --connect-timeout 10 --max-time 30 -X POST \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"$TEXERA_USERNAME\",\"password\":\"$TEXERA_PASSWORD\"}" \
        "${TEXERA_URL}/api/auth/login" 2>/dev/null)
    parse_response "$response"

    if is_success "$HTTP_CODE"; then
        JWT_TOKEN=$(json_value "$HTTP_BODY" "accessToken")
        if [ -n "$JWT_TOKEN" ]; then
            AUTH_HEADER="Authorization: Bearer $JWT_TOKEN"
            log_info "Login successful!"
            return 0
        fi
    fi
    log_error "Failed to authenticate. Check username and password."
    exit 1
}

get_dataset_id() {
    local dataset_name="$1"
    local response
    response=$(http_request GET "${TEXERA_FILE_SERVICE_URL}/api/dataset/list")
    parse_response "$response"

    if is_success "$HTTP_CODE" && [ -n "$HTTP_BODY" ] && [ "$HTTP_BODY" != "[]" ]; then
        if command -v jq >/dev/null 2>&1; then
            local did
            did=$(echo "$HTTP_BODY" | jq -r --arg name "$dataset_name" \
                'map(select(.dataset.name == $name)) | .[0].dataset.did // empty' 2>/dev/null)
            if [ -n "$did" ] && [ "$did" != "null" ]; then
                echo "$did"
                return 0
            fi
        else
            if echo "$HTTP_BODY" | grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${dataset_name}\""; then
                echo "$HTTP_BODY" | grep -o "\"did\"[[:space:]]*:[[:space:]]*[0-9]*" | head -n1 | grep -o "[0-9]*"
                return 0
            fi
        fi
    fi
    return 1
}

create_dataset() {
    local name="$1" description="$2"
    log_info "Creating dataset: $name" >&2

    local response
    response=$(http_request POST "${TEXERA_FILE_SERVICE_URL}/api/dataset/create" \
        "{\"datasetName\":\"$name\",\"datasetDescription\":\"$description\",\"isDatasetPublic\":true,\"isDatasetDownloadable\":true}")
    parse_response "$response"

    if is_success "$HTTP_CODE"; then
        log_info "Dataset '$name' created" >&2
        if command -v jq >/dev/null 2>&1; then
            echo "$HTTP_BODY" | jq -r ".dataset.did // empty" 2>/dev/null
        else
            echo "$HTTP_BODY" | grep -o "\"did\"[[:space:]]*:[[:space:]]*[0-9]*" | head -n1 | grep -o "[0-9]*"
        fi
        return 0
    fi
    log_error "Failed to create dataset '$name' (HTTP $HTTP_CODE)" >&2
    return 1
}

upload_file() {
    local did="$1" file_path="$2" remote_name="$3"
    log_info "Uploading: $remote_name"

    local encoded_path
    if command -v jq >/dev/null 2>&1; then
        encoded_path=$(printf '%s' "$remote_name" | jq -sRr @uri)
    else
        encoded_path="$remote_name"
    fi
    local file_size
    file_size=$(wc -c < "$file_path" | tr -d ' ')

    local response
    response=$(curl -s -w "\n%{http_code}" --connect-timeout 10 --max-time 120 -X POST \
        -H "$AUTH_HEADER" -H "Content-Type: application/octet-stream" -H "Content-Length: $file_size" \
        --data-binary "@$file_path" \
        "${TEXERA_FILE_SERVICE_URL}/api/dataset/${did}/upload?filePath=${encoded_path}" 2>/dev/null)
    parse_response "$response"

    if is_success "$HTTP_CODE"; then
        log_info "Uploaded: $remote_name"
        return 0
    fi
    log_error "Failed to upload $remote_name (HTTP $HTTP_CODE)"
    return 1
}

create_version() {
    local did="$1"
    log_info "Creating dataset version..."

    local response
    response=$(http_request POST "${TEXERA_FILE_SERVICE_URL}/api/dataset/${did}/version/create" "" "text/plain")
    parse_response "$response"

    if is_success "$HTTP_CODE"; then
        local version=""
        if command -v jq >/dev/null 2>&1; then
            version=$(echo "$HTTP_BODY" | jq -r '.datasetVersion.name // empty' 2>/dev/null)
        else
            version=$(echo "$HTTP_BODY" | grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' | head -n1 | sed 's/.*"\([^"]*\)"$/\1/')
        fi
        if [ -n "$version" ]; then
            log_info "Created version: $version"
            return 0
        fi
    fi
    log_error "Failed to create version (HTTP $HTTP_CODE)"
    return 1
}

process_dataset() {
    local dataset_name="$1" description="$2" data_dir="$3"

    local did
    did=$(get_dataset_id "$dataset_name") || true
    if [ -n "$did" ]; then
        log_info "Dataset '$dataset_name' exists (did: $did), skipping"
        return 0
    fi

    did=$(create_dataset "$dataset_name" "$description") || true
    if [ -z "$did" ]; then
        return 1
    fi

    local files_count=0
    for data_file in "$data_dir"/*.csv "$data_dir"/*.CSV "$data_dir"/*.txt "$data_dir"/*.tsv; do
        [ ! -f "$data_file" ] && continue
        local filename
        filename=$(basename "$data_file")
        [ "$filename" = "description.txt" ] && continue

        if upload_file "$did" "$data_file" "$filename"; then
            files_count=$((files_count + 1))
        fi
    done

    if [ "$files_count" -gt 0 ]; then
        create_version "$did" || true
    fi
    return 0
}

load_datasets() {
    [ "$SKIP_DATASETS" = "true" ] && log_info "Skipping datasets" && return 0
    [ ! -d "$DATASET_DIR" ] && log_warning "Dataset directory not found: $DATASET_DIR" && return 0

    log_info "Loading datasets from $DATASET_DIR..."
    local count=0

    for subdir in "$DATASET_DIR"/*; do
        [ ! -d "$subdir" ] && continue
        local name
        name=$(basename "$subdir")
        local desc="Example dataset: $name"
        [ -f "$subdir/description.txt" ] && desc=$(cat "$subdir/description.txt" 2>/dev/null || echo "$desc")

        if process_dataset "$name" "$desc" "$subdir"; then
            count=$((count + 1))
        fi
    done

    log_info "Processed $count dataset(s)"
}

workflow_exists() {
    local name="$1"
    local response
    response=$(http_request GET "${TEXERA_URL}/api/workflow/list")
    parse_response "$response"
    is_success "$HTTP_CODE" && echo "$HTTP_BODY" | grep -q "\"name\"[[:space:]]*:[[:space:]]*\"${name}\""
}

create_workflow() {
    local file="$1" name="$2" desc="$3"
    log_info "Creating workflow: $name"

    local content
    content=$(cat "$file")

    local escaped_content
    if command -v jq >/dev/null 2>&1; then
        escaped_content=$(echo "$content" | jq -c '.' | jq -Rs '.')
    else
        escaped_content=$(echo "$content" | sed 's/\\/\\\\/g; s/"/\\"/g; s/$/\\n/' | tr -d '\n')
        escaped_content="\"$escaped_content\""
    fi

    local payload
    if command -v jq >/dev/null 2>&1; then
        payload=$(jq -n --arg n "$name" --arg d "$desc" --argjson c "$escaped_content" \
            '{name: $n, description: $d, content: $c, isPublic: true}')
    else
        payload="{\"name\":\"$name\",\"description\":\"$desc\",\"content\":$escaped_content,\"isPublic\":true}"
    fi

    local response
    response=$(http_request POST "${TEXERA_URL}/api/workflow/persist" "$payload")
    parse_response "$response"

    if is_success "$HTTP_CODE"; then
        log_info "Created workflow: $name"
        return 0
    fi
    log_error "Failed to create workflow '$name' (HTTP $HTTP_CODE)"
    return 1
}

load_workflows() {
    [ "$SKIP_WORKFLOWS" = "true" ] && log_info "Skipping workflows" && return 0
    [ ! -d "$WORKFLOW_DIR" ] && log_warning "Workflow directory not found: $WORKFLOW_DIR" && return 0

    log_info "Loading workflows from $WORKFLOW_DIR..."
    local count=0

    for file in "$WORKFLOW_DIR"/*.json; do
        [ ! -f "$file" ] && continue
        local name
        name=$(basename "$file" .json)
        local desc="Example workflow: $name"

        if workflow_exists "$name"; then
            log_info "Workflow '$name' exists, skipping"
            continue
        fi
        if create_workflow "$file" "$name" "$desc"; then
            count=$((count + 1))
        fi
    done

    log_info "Processed $count workflow(s)"
}

check_services() {
    [ "$SKIP_SERVICE_CHECK" = "true" ] && return 0
    log_info "Checking services..."

    if [ "$SKIP_DATASETS" != "true" ]; then
        local response
        response=$(http_request GET "${TEXERA_FILE_SERVICE_URL}/api/dataset/list")
        parse_response "$response"
        [ "$HTTP_CODE" = "404" ] && log_error "Dataset API unavailable" && exit 1
        is_success "$HTTP_CODE" && log_info "Dataset API available"
    fi

    if [ "$SKIP_WORKFLOWS" != "true" ]; then
        local response
        response=$(http_request GET "${TEXERA_URL}/api/workflow/list")
        parse_response "$response"
        is_success "$HTTP_CODE" && log_info "Workflow API available"
    fi
}

main() {
    echo "============================================================"
    echo "Texera Example Data Loader"
    echo "============================================================"
    echo "Server: $TEXERA_URL"
    echo "File Service: $TEXERA_FILE_SERVICE_URL"
    echo "Username: $TEXERA_USERNAME"
    echo "============================================================"

    authenticate
    check_services
    load_datasets
    load_workflows

    echo ""
    log_info "Done!"
}

main
