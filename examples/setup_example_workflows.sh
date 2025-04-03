#!/bin/bash

# This script sets up example workflows in Texera

# Configuration
TEXERA_WEB_APPLICATION_URL=${TEXERA_WEB_APPLICATION_URL:-"http://localhost:8080/api"}
USERNAME=${TEXERA_EXAMPLE_USERNAME:-"texera"}
PASSWORD=${TEXERA_EXAMPLE_PASSWORD:-"texera"}
WORKFLOW_DIR=${TEXERA_EXAMPLE_WORKFLOW_DIR:-"workflows"}

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if workflow directory exists
if [ ! -d "$WORKFLOW_DIR" ]; then
    print_error "Workflow directory not found: $WORKFLOW_DIR"
    exit 1
fi

# Step 1: Login and get JWT token
print_status "Logging in user: $USERNAME"
LOGIN_RESPONSE=$(curl -s -X POST "$TEXERA_WEB_APPLICATION_URL/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")

if [[ $LOGIN_RESPONSE == *"accessToken"* ]]; then
    TOKEN=$(echo "$LOGIN_RESPONSE" | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)
    print_status "Login successful"
else
    print_status "User doesn't exist, attempting to register..."
    REGISTER_RESPONSE=$(curl -s -X POST "$TEXERA_WEB_APPLICATION_URL/auth/register" \
        -H "Content-Type: application/json" \
        -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")

    if [[ $REGISTER_RESPONSE == *"accessToken"* ]]; then
        TOKEN=$(echo "$REGISTER_RESPONSE" | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)
        print_status "Registration successful"
    else
        print_error "Registration failed"
        exit 1
    fi
fi

# Step 2: Get list of existing workflows
print_status "Fetching list of existing workflows..."
WORKFLOW_LIST_RESPONSE=$(curl -s -X GET "$TEXERA_WEB_APPLICATION_URL/workflow/list" \
    -H "Authorization: Bearer $TOKEN")

# Step 3: Process each JSON file
for workflow_file in "$WORKFLOW_DIR"/*.json; do
    if [ -f "$workflow_file" ]; then
        workflow_name=$(basename "$workflow_file" .json)
        print_status "Processing workflow: $workflow_name"

        if [[ $WORKFLOW_LIST_RESPONSE == *"\"name\":\"$workflow_name\""* ]]; then
            print_status "Workflow '$workflow_name' already exists, skipping"
            continue
        fi

        content=$(jq -c . "$workflow_file")
        if [ $? -ne 0 ]; then
            print_error "Failed to parse $workflow_file with jq"
            continue
        fi

        print_status "Creating workflow: $workflow_name"
        response=$(curl -s -X POST "$TEXERA_WEB_APPLICATION_URL/workflow/create" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            -d "{\"name\":\"$workflow_name\", \"content\": $(
                jq -Rs <<<"$content"
            )}")

        if [[ $response == *"wid"* ]]; then
            wid=$(echo $response | grep -o '"wid":[0-9]*' | cut -d':' -f2)
            print_status "Workflow '$workflow_name' created with ID $wid"
        else
            print_error "Failed to create workflow '$workflow_name'"
            echo "Response: $response"
        fi
    fi
done

print_status "Workflow upload process completed!"
