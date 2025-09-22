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

# Health check for web application
print_status "Checking health of web application..."
while true; do
    HEALTH_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "$TEXERA_WEB_APPLICATION_URL/healthcheck")
    if [ "$HEALTH_RESPONSE" == "200" ]; then
        print_status "Web application is healthy!"
        break
    else
        print_status "Waiting for web application to be ready... (Status: $HEALTH_RESPONSE)"
        sleep 2
    fi
done

# Check if workflow directory exists
if [ ! -d "$WORKFLOW_DIR" ]; then
    print_error "Workflow directory not found: $WORKFLOW_DIR"
    exit 1
fi

# Step 1: Login and get JWT token with retry logic
MAX_LOGIN_ATTEMPTS=5
LOGIN_ATTEMPT=1

while [ $LOGIN_ATTEMPT -le $MAX_LOGIN_ATTEMPTS ]; do
    print_status "Attempting to login user: $USERNAME (attempt $LOGIN_ATTEMPT/$MAX_LOGIN_ATTEMPTS)"
    
    # Try to login
    LOGIN_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$TEXERA_WEB_APPLICATION_URL/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")
    
    HTTP_CODE=$(echo "$LOGIN_RESPONSE" | tail -n 1)
    RESPONSE_BODY=$(echo "$LOGIN_RESPONSE" | head -n -1)
    
    if [ "$HTTP_CODE" == "200" ]; then
        TOKEN=$(echo "$RESPONSE_BODY" | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)
        print_status "Login successful"
        break
    elif [ "$HTTP_CODE" == "401" ] || [ "$HTTP_CODE" == "404" ]; then
        # User doesn't exist or wrong credentials, try to register
        print_status "Login failed (HTTP $HTTP_CODE), attempting to register..."
        
        REGISTER_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$TEXERA_WEB_APPLICATION_URL/auth/register" \
            -H "Content-Type: application/json" \
            -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")
        
        REG_HTTP_CODE=$(echo "$REGISTER_RESPONSE" | tail -n 1)
        REG_RESPONSE_BODY=$(echo "$REGISTER_RESPONSE" | head -n -1)
        
        if [ "$REG_HTTP_CODE" == "200" ] || [ "$REG_HTTP_CODE" == "201" ]; then
            TOKEN=$(echo "$REG_RESPONSE_BODY" | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)
            print_status "Registration successful"
            break
        else
            print_error "Registration failed (HTTP $REG_HTTP_CODE)"
        fi
    fi
    
    if [ $LOGIN_ATTEMPT -lt $MAX_LOGIN_ATTEMPTS ]; then
        print_status "Waiting 5 seconds before retry..."
        sleep 5
    fi
    
    LOGIN_ATTEMPT=$((LOGIN_ATTEMPT + 1))
done

if [ -z "$TOKEN" ]; then
    print_error "Failed to authenticate after $MAX_LOGIN_ATTEMPTS attempts"
    exit 1
fi

# Step 2: Get list of existing workflows
print_status "Fetching list of existing workflows..."
WORKFLOW_LIST_RESPONSE=$(curl -s -w "\n%{http_code}" -X GET "$TEXERA_WEB_APPLICATION_URL/workflow/list" \
    -H "Authorization: Bearer $TOKEN")

LIST_HTTP_CODE=$(echo "$WORKFLOW_LIST_RESPONSE" | tail -n 1)
LIST_BODY=$(echo "$WORKFLOW_LIST_RESPONSE" | head -n -1)

if [ "$LIST_HTTP_CODE" != "200" ]; then
    print_error "Failed to fetch workflow list (HTTP $LIST_HTTP_CODE)"
    exit 1
fi

# Step 3: Process each JSON file
for workflow_file in "$WORKFLOW_DIR"/*.json; do
    if [ -f "$workflow_file" ]; then
        workflow_name=$(basename "$workflow_file" .json)
        print_status "Processing workflow: $workflow_name"

        if [[ $LIST_BODY == *"\"name\":\"$workflow_name\""* ]]; then
            print_status "Workflow '$workflow_name' already exists, skipping"
            continue
        fi

        content=$(jq -c . "$workflow_file")
        if [ $? -ne 0 ]; then
            print_error "Failed to parse $workflow_file with jq"
            continue
        fi

        print_status "Creating workflow: $workflow_name"
        CREATE_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$TEXERA_WEB_APPLICATION_URL/workflow/create" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/json" \
            -d "{\"name\":\"$workflow_name\", \"content\": $(
                jq -Rs <<<"$content"
            )}")

        CREATE_HTTP_CODE=$(echo "$CREATE_RESPONSE" | tail -n 1)
        CREATE_BODY=$(echo "$CREATE_RESPONSE" | head -n -1)

        if [ "$CREATE_HTTP_CODE" == "200" ] || [ "$CREATE_HTTP_CODE" == "201" ]; then
            wid=$(echo "$CREATE_BODY" | grep -o '"wid":[0-9]*' | cut -d':' -f2)
            print_status "Workflow '$workflow_name' created successfully with ID $wid"
        else
            print_error "Failed to create workflow '$workflow_name' (HTTP $CREATE_HTTP_CODE)"
        fi
    fi
done

print_status "Workflow upload process completed!"
