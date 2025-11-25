# Texera Example Data Loader

This directory contains Python scripts for loading example datasets and workflows into Texera.

## Overview

The example loader provides a Python-based approach to:
- Authenticate with the Texera server
- Create datasets and upload files using multipart upload
- Create workflows from JSON definitions

## Directory Structure

```
bin/example-data-loader/
├── dataset/                    # Example datasets (organized by subdirectories)
│   ├── iris-species/           # Example dataset directory
│   │   ├── Iris.csv            # Data file(s)
│   │   └── description         # Optional description file
│   └── popular-movies/         # Another example dataset
│       ├── movies.csv          # Data file(s)
│       └── description         # Optional description file
├── workflow/                   # Example workflow JSON files
├── login.py                    # Authentication module
├── load_dataset.py             # Dataset loading with multipart upload
├── load_workflow.py            # Workflow loading module
├── main.py                     # Main orchestration script
├── requirements.txt            # Python dependencies
└── README.md                   # This file
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

The loader supports two directory structures:

#### Recommended: Subdirectory Structure

1. Create a subdirectory in `dataset/` with your dataset name (e.g., `dataset/my-dataset/`)
2. Add your data files to this subdirectory (CSV, JSON, TXT, TSV, etc.)
3. Optionally, create a `description` file with a text description of the dataset
4. Run the loader - a dataset will be created with the subdirectory name, and all data files will be uploaded

**Example:**
```
dataset/
└── customer-data/
    ├── customers.csv
    ├── orders.csv
    └── description          # Contains: "Customer and order data from 2024"
```

This will create a dataset named `customer-data` with the description from the file, containing both `customers.csv` and `orders.csv`.

#### Legacy: Flat File Structure

1. Place your data files (CSV, JSON, TXT) directly in the `dataset/` directory
2. Run the loader - datasets will be created automatically with the filename as the dataset name

**Note:** The subdirectory structure is preferred as it supports:
- Multiple files per dataset
- Custom descriptions
- Better organization

### Adding Workflows

1. Create a workflow JSON file in the `workflow/` directory
2. Run the loader - workflows will be created with the filename as the workflow name
