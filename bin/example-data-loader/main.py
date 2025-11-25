#!/usr/bin/env python3
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

"""
Main script to load example datasets and workflows into Texera.

This script:
1. Authenticates with Texera server
2. Creates datasets and uploads example data files
3. Creates workflows from JSON files
"""

import os
import sys
import argparse
import logging
from pathlib import Path

from login import TexeraAuth
from load_dataset import TexeraDatasetLoader
from load_workflow import TexeraWorkflowLoader


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_datasets(
    dataset_loader: TexeraDatasetLoader,
    dataset_dir: Path,
    owner_email: str
):
    """
    Load all datasets from the dataset directory.

    Supports two structures:
    1. Subdirectories: dataset/dataset-name/ contains data files and optional "description" file
    2. Flat files: dataset/*.csv directly in dataset directory (legacy)

    Args:
        dataset_loader: DatasetLoader instance
        dataset_dir: Path to dataset directory
        owner_email: Email of the dataset owner
    """
    logger = logging.getLogger(__name__)

    if not dataset_dir.exists():
        logger.warning(f"Dataset directory not found: {dataset_dir}")
        return

    # Find all subdirectories (new structure)
    subdirs = [d for d in dataset_dir.iterdir() if d.is_dir()]

    # Find all data files directly in dataset directory (legacy structure)
    direct_files = list(dataset_dir.glob('*.csv')) + \
                   list(dataset_dir.glob('*.CSV')) + \
                   list(dataset_dir.glob('*.json')) + \
                   list(dataset_dir.glob('*.txt'))

    if not subdirs and not direct_files:
        logger.warning(f"No dataset subdirectories or data files found in {dataset_dir}")
        return

    datasets_processed = 0

    # Process subdirectory-based datasets (preferred structure)
    for subdir in subdirs:
        dataset_name = subdir.name
        logger.info(f"Processing dataset directory: {dataset_name}")

        # Look for description file
        description_file = subdir / "description"
        if description_file.exists():
            try:
                with open(description_file, 'r', encoding='utf-8') as f:
                    dataset_description = f.read().strip()
            except Exception as e:
                logger.warning(f"Failed to read description file for {dataset_name}: {e}")
                dataset_description = f"Example dataset: {dataset_name}"
        else:
            dataset_description = f"Example dataset: {dataset_name}"

        # Find all data files in the subdirectory
        data_files = list(subdir.glob('*.csv')) + \
                     list(subdir.glob('*.CSV')) + \
                     list(subdir.glob('*.json')) + \
                     list(subdir.glob('*.JSON')) + \
                     list(subdir.glob('*.txt')) + \
                     list(subdir.glob('*.tsv'))

        # Exclude description file
        data_files = [f for f in data_files if f.name != "description"]

        if not data_files:
            logger.warning(f"No data files found in {subdir}, skipping")
            continue

        logger.info(f"Found {len(data_files)} data file(s) in {dataset_name}")

        # Check if dataset already exists
        existing = dataset_loader.get_dataset_by_name(dataset_name)

        if existing:
            logger.info(f"Dataset '{dataset_name}' already exists, skipping creation")
        else:
            # Create dataset
            try:
                dataset_loader.create_dataset(
                    name=dataset_name,
                    description=dataset_description,
                    is_public=True,
                    is_downloadable=True
                )
                logger.info(f"Created dataset: {dataset_name}")
            except Exception as e:
                logger.error(f"Failed to create dataset {dataset_name}: {e}")
                continue

        # Upload all data files from this subdirectory
        for data_file in data_files:
            try:
                logger.info(f"Uploading file: {data_file.name}")
                dataset_loader.upload_file_multipart(
                    owner_email=owner_email,
                    dataset_name=dataset_name,
                    local_file_path=str(data_file),
                    remote_file_path=data_file.name
                )
                logger.info(f"Successfully uploaded {data_file.name} to dataset {dataset_name}")
            except Exception as e:
                logger.error(f"Failed to upload {data_file.name}: {e}")

        datasets_processed += 1

    # Process legacy flat-file structure (for backward compatibility)
    if direct_files:
        logger.info(f"Found {len(direct_files)} data files in legacy flat structure")
        for data_file in direct_files:
            dataset_name = data_file.stem.replace('_', '-')
            dataset_description = f"Example dataset: {data_file.stem}"

            logger.info(f"Processing legacy dataset: {dataset_name}")

            # Check if dataset already exists
            existing = dataset_loader.get_dataset_by_name(dataset_name)

            if existing:
                logger.info(f"Dataset '{dataset_name}' already exists, skipping creation")
            else:
                # Create dataset
                try:
                    dataset_loader.create_dataset(
                        name=dataset_name,
                        description=dataset_description,
                        is_public=True,
                        is_downloadable=True
                    )
                except Exception as e:
                    logger.error(f"Failed to create dataset {dataset_name}: {e}")
                    continue

            # Upload file using multipart upload
            try:
                logger.info(f"Uploading file: {data_file.name}")
                dataset_loader.upload_file_multipart(
                    owner_email=owner_email,
                    dataset_name=dataset_name,
                    local_file_path=str(data_file),
                    remote_file_path=data_file.name
                )
                logger.info(f"Successfully uploaded {data_file.name} to dataset {dataset_name}")
            except Exception as e:
                logger.error(f"Failed to upload {data_file.name}: {e}")

            datasets_processed += 1

    logger.info(f"Processed {datasets_processed} dataset(s)")


def load_workflows(
    workflow_loader: TexeraWorkflowLoader,
    workflow_dir: Path
):
    """
    Load all workflows from the workflow directory.

    Args:
        workflow_loader: WorkflowLoader instance
        workflow_dir: Path to workflow directory
    """
    logger = logging.getLogger(__name__)

    if not workflow_dir.exists():
        logger.warning(f"Workflow directory not found: {workflow_dir}")
        return

    # Find all JSON workflow files
    workflow_files = list(workflow_dir.glob('*.json'))

    if not workflow_files:
        logger.warning(f"No workflow files found in {workflow_dir}")
        return

    logger.info(f"Found {len(workflow_files)} workflow files to load")

    for workflow_file in workflow_files:
        workflow_name = workflow_file.stem.replace('_', ' ').title()
        workflow_description = f"Example workflow: {workflow_file.stem}"

        logger.info(f"Processing workflow: {workflow_name}")

        # Check if workflow already exists
        existing = workflow_loader.get_workflow_by_name(workflow_name)

        if existing:
            logger.info(f"Workflow '{workflow_name}' already exists, skipping")
            continue

        # Create workflow from file
        try:
            workflow_info = workflow_loader.load_workflow_from_file(
                file_path=str(workflow_file),
                name=workflow_name,
                description=workflow_description,
                is_public=True
            )
            logger.info(f"Successfully created workflow: {workflow_name}")
        except Exception as e:
            logger.error(f"Failed to create workflow from {workflow_file.name}: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Load example datasets and workflows into Texera'
    )
    parser.add_argument(
        '--url',
        default=os.getenv('TEXERA_URL', 'http://localhost:8080'),
        help='Texera server URL (default: http://localhost:8080 or TEXERA_URL env var)'
    )
    parser.add_argument(
        '--username',
        default=os.getenv('TEXERA_USERNAME', 'admin'),
        help='Username for authentication (default: admin or TEXERA_USERNAME env var)'
    )
    parser.add_argument(
        '--password',
        default=os.getenv('TEXERA_PASSWORD', 'admin'),
        help='Password for authentication (default: admin or TEXERA_PASSWORD env var)'
    )
    parser.add_argument(
        '--email',
        default=os.getenv('TEXERA_EMAIL'),
        help='Email address (defaults to username if not provided)'
    )
    parser.add_argument(
        '--dataset-dir',
        type=Path,
        default=Path(__file__).parent / 'dataset',
        help='Directory containing dataset files (default: ./dataset)'
    )
    parser.add_argument(
        '--workflow-dir',
        type=Path,
        default=Path(__file__).parent / 'workflow',
        help='Directory containing workflow files (default: ./workflow)'
    )
    parser.add_argument(
        '--skip-datasets',
        action='store_true',
        help='Skip loading datasets'
    )
    parser.add_argument(
        '--skip-workflows',
        action='store_true',
        help='Skip loading workflows'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Use username as email if email not provided
    email = args.email if args.email else args.username

    logger.info("=" * 60)
    logger.info("Texera Example Data Loader")
    logger.info("=" * 60)
    logger.info(f"Server URL: {args.url}")
    logger.info(f"Username: {args.username}")
    logger.info(f"Email: {email}")
    logger.info(f"Dataset directory: {args.dataset_dir}")
    logger.info(f"Workflow directory: {args.workflow_dir}")
    logger.info("=" * 60)

    try:
        # Step 1: Authenticate
        logger.info("Step 1: Authenticating with Texera server...")
        auth = TexeraAuth(args.url)

        try:
            token = auth.login(args.username, args.password)
        except Exception as login_error:
            logger.warning(f"Login failed: {login_error}")
            logger.info("Attempting to register new user...")
            try:
                token = auth.register(args.username, args.password)
            except Exception as register_error:
                logger.error(f"Registration also failed: {register_error}")
                logger.error("Unable to authenticate. Exiting.")
                sys.exit(1)

        logger.info("Authentication successful!")

        auth_headers = auth.get_headers()

        # Step 2: Load datasets
        if not args.skip_datasets:
            logger.info("\nStep 2: Loading datasets...")
            dataset_loader = TexeraDatasetLoader(args.url, auth_headers)
            load_datasets(dataset_loader, args.dataset_dir, email)
            logger.info("Dataset loading completed!")
        else:
            logger.info("\nStep 2: Skipping datasets (--skip-datasets)")

        # Step 3: Load workflows
        if not args.skip_workflows:
            logger.info("\nStep 3: Loading workflows...")
            workflow_loader = TexeraWorkflowLoader(args.url, auth_headers)
            load_workflows(workflow_loader, args.workflow_dir)
            logger.info("Workflow loading completed!")
        else:
            logger.info("\nStep 3: Skipping workflows (--skip-workflows)")

        logger.info("\n" + "=" * 60)
        logger.info("All examples loaded successfully!")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nFatal error: {e}", exc_info=args.verbose)
        sys.exit(1)


if __name__ == '__main__':
    main()
