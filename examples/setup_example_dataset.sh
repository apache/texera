#!/bin/bash

# This script sets up example datasets in Texera

TEXERA_WEB_APPLICATION_URL=${TEXERA_WEB_APPLICATION_URL:-"http://localhost:8080/api"}
TEXERA_FILE_SERVICE_URL=${TEXERA_FILE_SERVICE_URL:-"http://localhost:9092/api"}
USERNAME=${TEXERA_EXAMPLE_USERNAME:-"texera"}
PASSWORD=${TEXERA_EXAMPLE_PASSWORD:-"texera"}
DATASET_NAME=${TEXERA_EXAMPLE_DATASET_NAME:-"texera-example-dataset"}
DATASET_DESCRIPTION=${TEXERA_EXAMPLE_DATASET_DESCRIPTION:-"Example data for users to use Texera"}
IS_PUBLIC=${TEXERA_EXAMPLE_IS_PUBLIC:-"true"}
UPLOAD_DIR=${TEXERA_EXAMPLE_DATASET_DIR:-"datasets"}

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Step 1: Login and get JWT token
print_status "Logging in user: $USERNAME"
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

# Step 2: Check if dataset already exists
print_status "Checking if dataset '$DATASET_NAME' already exists..."
DATASET_LIST_RESPONSE=$(curl -s -X GET "$TEXERA_FILE_SERVICE_URL/dataset/list" \
    -H "Authorization: Bearer $TOKEN")

if [[ $DATASET_LIST_RESPONSE == *"\"name\":\"$DATASET_NAME\""* ]]; then
    print_status "Dataset '$DATASET_NAME' already exists, skipping creation"
    exit 0
fi

# Step 3: Create dataset
print_status "Creating dataset: $DATASET_NAME"
DATASET_RESPONSE=$(curl -s -X POST "$TEXERA_FILE_SERVICE_URL/dataset/create" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d "{
        \"datasetName\": \"$DATASET_NAME\",
        \"datasetDescription\": \"$DATASET_DESCRIPTION\",
        \"isDatasetPublic\": $IS_PUBLIC
    }")

DATASET_ID=$(echo $DATASET_RESPONSE | grep -o '"did":[0-9]*' | cut -d':' -f2)

if [ -z "$DATASET_ID" ]; then
    print_error "Failed to create dataset"
    exit 1
fi

print_status "Dataset created successfully with ID: $DATASET_ID"

# Step 4: Upload files
if [ -d "$UPLOAD_DIR" ]; then
    print_status "Uploading files from directory: $UPLOAD_DIR"
    for file in "$UPLOAD_DIR"/*; do
        if [ -f "$file" ]; then
            FILENAME=$(basename "$file")
            print_status "Uploading file: $FILENAME"
            ENCODED_PATH=$(echo "$FILENAME" | sed 's/ /%20/g')

            UPLOAD_RESPONSE=$(curl -s -X POST "$TEXERA_FILE_SERVICE_URL/dataset/$DATASET_ID/upload?filePath=$ENCODED_PATH" \
                -H "Authorization: Bearer $TOKEN" \
                -H "Content-Type: application/octet-stream" \
                --data-binary "@$file")

            if [[ $UPLOAD_RESPONSE == *"successfully"* ]]; then
                print_status "File $FILENAME uploaded successfully"
            else
                print_error "Failed to upload file $FILENAME"
            fi
        fi
    done
else
    print_status "Upload directory '$UPLOAD_DIR' not found, skipping file upload"
fi

# Step 5: Create version
print_status "Creating version for dataset"
VERSION_RESPONSE=$(curl -s -X POST "$TEXERA_FILE_SERVICE_URL/dataset/$DATASET_ID/version/create" \
    -H "Content-Type: text/plain" \
    -H "Authorization: Bearer $TOKEN" \
    -d "Initial version")

if [[ $VERSION_RESPONSE == *"datasetVersion"* ]]; then
    print_status "Version created successfully"
else
    print_error "Failed to create version"
    exit 1
fi

print_status "Setup completed successfully!"
