FROM alpine:3.19

# Install required tools
RUN apk add --no-cache bash curl jq coreutils

# Set working directory
WORKDIR /example_data

# Copy setup scripts
COPY examples/setup_example_dataset.sh .
COPY examples/setup_example_workflows.sh .

# Copy example data directories
COPY examples/datasets/ ./datasets/
COPY examples/workflows/ ./workflows/

# Make the scripts executable
RUN chmod +x setup_example_dataset.sh setup_example_workflows.sh

# Expose all configurable environment variables
ENV TEXERA_EXAMPLE_USERNAME=texera
ENV TEXERA_EXAMPLE_PASSWORD=texera
ENV TEXERA_EXAMPLE_DATASET_NAME=texera-example-dataset
ENV TEXERA_EXAMPLE_DATASET_DESCRIPTION="Example data for users to use Texera"
ENV TEXERA_EXAMPLE_IS_PUBLIC=true
ENV TEXERA_EXAMPLE_DATASET_DIR=datasets
ENV TEXERA_EXAMPLE_WORKFLOW_DIR=workflows
ENV TEXERA_WEB_APPLICATION_URL=http://texera-web-application:8080/api
ENV TEXERA_FILE_SERVICE_URL=http://file-service:9092/api

# Default command to run both scripts
CMD ["sh", "-c", "./setup_example_dataset.sh && ./setup_example_workflows.sh && echo 'Texera Example Data has been loaded successfully!'"]