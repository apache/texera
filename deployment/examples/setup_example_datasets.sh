#!/bin/bash

# Load configs from env or default
TEXERA_WEB_APPLICATION_URL=${TEXERA_WEB_APPLICATION_URL:-"http://localhost:8080/api"}
TEXERA_FILE_SERVICE_URL=${TEXERA_FILE_SERVICE_URL:-"http://localhost:9092/api"}
USERNAME=${TEXERA_EXAMPLE_USERNAME:-"texera"}
PASSWORD=${TEXERA_EXAMPLE_PASSWORD:-"texera"}
DATASET_DIR_ROOT=${TEXERA_EXAMPLE_DATASET_DIR:-"datasets"}

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

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

# Health check for file service
print_status "Checking health of file service..."
while true; do
    HEALTH_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "$TEXERA_FILE_SERVICE_URL/healthcheck")
    if [ "$HEALTH_RESPONSE" == "200" ]; then
        print_status "File service is healthy!"
        break
    else
        print_status "Waiting for file service to be ready... (Status: $HEALTH_RESPONSE)"
        sleep 2
    fi
done

# Step 1: Authenticate with retry logic
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

# Step 2: Loop through dataset folders
if [ ! -d "$DATASET_DIR_ROOT" ]; then
    print_error "Dataset root folder '$DATASET_DIR_ROOT' not found."
    exit 1
fi

for dataset_folder in "$DATASET_DIR_ROOT"/*; do
    if [ -d "$dataset_folder" ]; then
        DATASET_NAME=$(basename "$dataset_folder")
        DESCRIPTION_FILE="$dataset_folder/description"
        DATASET_DESCRIPTION=""

        if [ -f "$DESCRIPTION_FILE" ]; then
            DATASET_DESCRIPTION=$(<"$DESCRIPTION_FILE")
        fi

        print_status "Processing dataset: $DATASET_NAME"

        # Check if it already exists
        LIST_RESPONSE=$(curl -s -w "\n%{http_code}" -X GET "$TEXERA_FILE_SERVICE_URL/dataset/list" \
            -H "Authorization: Bearer $TOKEN")
        
        LIST_HTTP_CODE=$(echo "$LIST_RESPONSE" | tail -n 1)
        LIST_BODY=$(echo "$LIST_RESPONSE" | head -n -1)
        
        if [ "$LIST_HTTP_CODE" == "200" ] && [[ $LIST_BODY == *"\"name\":\"$DATASET_NAME\""* ]]; then
            print_status "Dataset '$DATASET_NAME' already exists, skipping"
            continue
        fi

        # Create dataset
        CREATE_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$TEXERA_FILE_SERVICE_URL/dataset/create" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TOKEN" \
            -d "{
                \"datasetName\": \"$DATASET_NAME\",
                \"datasetDescription\": \"$DATASET_DESCRIPTION\",
                \"isDatasetPublic\": true
            }")

        CREATE_HTTP_CODE=$(echo "$CREATE_RESPONSE" | tail -n 1)
        CREATE_BODY=$(echo "$CREATE_RESPONSE" | head -n -1)
        
        if [ "$CREATE_HTTP_CODE" != "200" ] && [ "$CREATE_HTTP_CODE" != "201" ]; then
            print_error "Failed to create dataset '$DATASET_NAME' (HTTP $CREATE_HTTP_CODE)"
            continue
        fi
        
        DATASET_ID=$(echo "$CREATE_BODY" | grep -o '"did":[0-9]*' | cut -d':' -f2)
        
        if [ -z "$DATASET_ID" ]; then
            print_error "Failed to extract dataset ID for '$DATASET_NAME'"
            continue
        fi

        print_status "Created dataset '$DATASET_NAME' with ID $DATASET_ID"

        # Upload files (exclude description)
        for file in "$dataset_folder"/*; do
            FILENAME=$(basename "$file")
            if [[ -f "$file" && "$FILENAME" != "description" ]]; then
                print_status "Uploading file: $FILENAME"
                ENCODED_NAME=$(echo "$FILENAME" | sed 's/ /%20/g')

                HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$TEXERA_FILE_SERVICE_URL/dataset/$DATASET_ID/upload?filePath=$ENCODED_NAME&useMultipartUpload=false" \
                    -H "Authorization: Bearer $TOKEN" \
                    -H "Content-Type: application/octet-stream" \
                    --data-binary "@$file")

                if [[ "$HTTP_CODE" == "200" ]]; then
                    print_status "Uploaded $FILENAME"
                else
                    print_error "Failed to upload $FILENAME (HTTP $HTTP_CODE)"
                fi
            fi
        done

        # Create version
        print_status "Creating version for $DATASET_NAME"
        VERSION_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$TEXERA_FILE_SERVICE_URL/dataset/$DATASET_ID/version/create" \
            -H "Content-Type: text/plain" \
            -H "Authorization: Bearer $TOKEN" \
            -d "")

        VERSION_HTTP_CODE=$(echo "$VERSION_RESPONSE" | tail -n 1)
        VERSION_BODY=$(echo "$VERSION_RESPONSE" | head -n -1)
        
        if [ "$VERSION_HTTP_CODE" == "200" ] || [ "$VERSION_HTTP_CODE" == "201" ]; then
            print_status "Version created successfully for $DATASET_NAME"
        else
            print_error "Failed to create version for $DATASET_NAME (HTTP $VERSION_HTTP_CODE)"
        fi
    fi
done

print_status "All datasets processed."