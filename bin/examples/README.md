# Texera Example Data Loader

This directory contains Python scripts for loading example datasets and workflows into Texera.

## Overview

The example loader provides a Python-based approach to:
- Authenticate with the Texera server
- Create datasets and upload files using multipart upload
- Create workflows from JSON definitions

## Directory Structure

```
bin/examples/
├── dataset/              # Example dataset files (CSV, JSON, etc.)
├── workflow/             # Example workflow JSON files
├── login.py             # Authentication module
├── load_dataset.py      # Dataset loading with multipart upload
├── load_workflow.py     # Workflow loading module
├── main.py              # Main orchestration script
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

## Requirements

- Python 3.7 or higher
- Access to a running Texera server

## Installation

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Load all examples with default settings (localhost:8080, admin/admin):

```bash
python main.py
```

### Custom Server and Credentials

```bash
python main.py \
  --url http://texera-server:8080 \
  --username myuser \
  --password mypassword \
  --email myuser@example.com
```

### Using Environment Variables

```bash
export TEXERA_URL=http://texera-server:8080
export TEXERA_USERNAME=myuser
export TEXERA_PASSWORD=mypassword
export TEXERA_EMAIL=myuser@example.com

python main.py
```

### Skip Datasets or Workflows

```bash
# Load only datasets
python main.py --skip-workflows

# Load only workflows
python main.py --skip-datasets
```

### Custom Directories

```bash
python main.py \
  --dataset-dir /path/to/datasets \
  --workflow-dir /path/to/workflows
```

### Verbose Logging

```bash
python main.py --verbose
```

## Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--url` | `http://localhost:8080` | Texera server URL |
| `--username` | `admin` | Username for authentication |
| `--password` | `admin` | Password for authentication |
| `--email` | (username) | Email address for dataset ownership |
| `--dataset-dir` | `./dataset` | Directory containing dataset files |
| `--workflow-dir` | `./workflow` | Directory containing workflow JSON files |
| `--skip-datasets` | False | Skip loading datasets |
| `--skip-workflows` | False | Skip loading workflows |
| `--verbose` | False | Enable verbose logging |

## Adding Your Own Examples

### Adding Datasets

1. Place your data files (CSV, JSON, TXT) in the `dataset/` directory
2. Run the loader - datasets will be created automatically with the filename as the dataset name

### Adding Workflows

1. Create a workflow JSON file in the `workflow/` directory
2. The workflow JSON should follow the Texera workflow schema:

```json
{
  "operators": [
    {
      "operatorID": "operator-1",
      "operatorType": "CSVFileScan",
      "operatorProperties": {
        "fileName": "example.csv",
        "hasHeader": true
      },
      ...
    }
  ],
  "links": [],
  "breakpoints": {},
  "commentBoxes": []
}
```

3. Run the loader - workflows will be created with the filename as the workflow name

## Features

### Multipart Upload

The dataset loader uses S3-compatible multipart upload for efficient transfer of large files:

- Automatic part size calculation based on file size
- Concurrent part uploads (default: 10 concurrent parts)
- Automatic retry and abort on failure
- Progress tracking

### Authentication

The loader supports:
- Login with existing credentials
- Automatic registration if login fails
- JWT token-based authentication for all API calls

### Error Handling

- Graceful handling of existing datasets and workflows (skips duplicates)
- Detailed error logging
- Automatic cleanup on upload failures

## Module Documentation

### login.py

```python
from login import TexeraAuth

auth = TexeraAuth("http://localhost:8080")
token = auth.login("username", "password")
headers = auth.get_headers()
```

### load_dataset.py

```python
from load_dataset import TexeraDatasetLoader

loader = TexeraDatasetLoader("http://localhost:8080", auth_headers)

# Create dataset
dataset = loader.create_dataset(
    name="my-dataset",
    description="My dataset",
    is_public=True
)

# Upload file with multipart
loader.upload_file_multipart(
    owner_email="user@example.com",
    dataset_name="my-dataset",
    local_file_path="/path/to/file.csv"
)
```

### load_workflow.py

```python
from load_workflow import TexeraWorkflowLoader

loader = TexeraWorkflowLoader("http://localhost:8080", auth_headers)

# Load workflow from file
workflow = loader.load_workflow_from_file(
    file_path="/path/to/workflow.json",
    name="My Workflow",
    is_public=True
)
```

## Docker Usage

See `texera-examples-loader.dockerfile` for containerized execution.

## License

Apache License 2.0
