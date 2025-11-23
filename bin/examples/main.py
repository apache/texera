#!/usr/bin/env python3
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

    Args:
        dataset_loader: DatasetLoader instance
        dataset_dir: Path to dataset directory
        owner_email: Email of the dataset owner
    """
    logger = logging.getLogger(__name__)

    if not dataset_dir.exists():
        logger.warning(f"Dataset directory not found: {dataset_dir}")
        return

    # Find all CSV/data files in dataset directory
    data_files = list(dataset_dir.glob('*.csv')) + \
                 list(dataset_dir.glob('*.json')) + \
                 list(dataset_dir.glob('*.txt'))

    if not data_files:
        logger.warning(f"No data files found in {dataset_dir}")
        return

    logger.info(f"Found {len(data_files)} data files to upload")

    for data_file in data_files:
        dataset_name = data_file.stem.replace('_', '-')
        dataset_description = f"Example dataset: {data_file.stem}"

        logger.info(f"Processing dataset: {dataset_name}")

        # Check if dataset already exists
        existing = dataset_loader.get_dataset_by_name(dataset_name)

        if existing:
            logger.info(f"Dataset '{dataset_name}' already exists, skipping creation")
            dataset_info = existing
        else:
            # Create dataset
            dataset_info = dataset_loader.create_dataset(
                name=dataset_name,
                description=dataset_description,
                is_public=True,
                is_downloadable=True
            )

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
