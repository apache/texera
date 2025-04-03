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

# Step 1: Authenticate
print_status "Logging in as $USERNAME"
LOGIN_RESPONSE=$(curl -s -X POST "$TEXERA_WEB_APPLICATION_URL/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")

if [[ $LOGIN_RESPONSE == *"accessToken"* ]]; then
    TOKEN=$(echo $LOGIN_RESPONSE | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)
    print_status "Login successful"
else
    print_status "User doesn't exist, attempting to register..."
    REGISTER_RESPONSE=$(curl -s -X POST "$TEXERA_WEB_APPLICATION_URL/auth/register" \
        -H "Content-Type: application/json" \
        -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\"}")
    
    if [[ $REGISTER_RESPONSE == *"accessToken"* ]]; then
        TOKEN=$(echo $REGISTER_RESPONSE | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)
        print_status "Registration successful"
    else
        print_error "Registration failed"
        exit 1
    fi
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
        LIST_RESPONSE=$(curl -s -X GET "$TEXERA_FILE_SERVICE_URL/dataset/list" \
            -H "Authorization: Bearer $TOKEN")
        
        if [[ $LIST_RESPONSE == *"\"name\":\"$DATASET_NAME\""* ]]; then
            print_status "Dataset '$DATASET_NAME' already exists, skipping"
            continue
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

        DATASET_ID=$(echo $CREATE_RESPONSE | grep -o '"did":[0-9]*' | cut -d':' -f2)

        if [ -z "$DATASET_ID" ]; then
            print_error "Failed to create dataset '$DATASET_NAME'"
            continue
        fi

        print_status "Created dataset '$DATASET_NAME' with ID $DATASET_ID"

        # Upload files (exclude description)
        for file in "$dataset_folder"/*; do
            FILENAME=$(basename "$file")
            if [[ -f "$file" && "$FILENAME" != "description" ]]; then
                print_status "Uploading file: $FILENAME"
                ENCODED_NAME=$(echo "$FILENAME" | sed 's/ /%20/g')

                UPLOAD_RESPONSE=$(curl -s -X POST "$TEXERA_FILE_SERVICE_URL/dataset/$DATASET_ID/upload?filePath=$ENCODED_NAME" \
                    -H "Authorization: Bearer $TOKEN" \
                    -H "Content-Type: application/octet-stream" \
                    --data-binary "@$file")

                if [[ $UPLOAD_RESPONSE == *"successfully"* ]]; then
                    print_status "Uploaded $FILENAME"
                else
                    print_error "Failed to upload $FILENAME"
                fi
            fi
        done

        # Create version
        print_status "Creating version for $DATASET_NAME"
        VERSION_RESPONSE=$(curl -s -X POST "$TEXERA_FILE_SERVICE_URL/dataset/$DATASET_ID/version/create" \
            -H "Content-Type: text/plain" \
            -H "Authorization: Bearer $TOKEN" \
            -d "")

        if [[ $VERSION_RESPONSE == *"datasetVersion"* ]]; then
            print_status "Version created successfully"
        else
            print_error "Failed to create version for $DATASET_NAME"
        fi
    fi
done

print_status "All datasets processed."