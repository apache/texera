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
Workflow management module for Texera API.
Handles workflow creation and management.
"""

import requests
import json
import os
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class TexeraWorkflowLoader:
    """Handle workflow operations with Texera server."""

    def __init__(self, base_url: str, auth_headers: dict):
        """
        Initialize workflow loader.

        Args:
            base_url: Base URL of Texera server
            auth_headers: Authorization headers from TexeraAuth
        """
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api"
        self.auth_headers = auth_headers

    def create_workflow(
        self,
        name: str,
        description: str = "",
        content: Optional[Dict[str, Any]] = None,
        is_public: bool = False
    ) -> Dict[str, Any]:
        """
        Create a new workflow.

        Args:
            name: Workflow name
            description: Workflow description
            content: Workflow content (operators, links, etc.)
            is_public: Whether workflow is public

        Returns:
            Created workflow information

        Raises:
            Exception: If workflow creation fails
        """
        url = f"{self.api_url}/workflow/create"

        # Default empty workflow
        if content is None:
            content = {
                "operators": [],
                "links": [],
                "breakpoints": {},
                "commentBoxes": []
            }

        payload = {
            "wid": None,
            "name": name,
            "description": description,
            "content": json.dumps(content),
            "creationTime": None,
            "lastModifiedTime": None,
            "isPublic": is_public
        }

        logger.info(f"Creating workflow: {name}")

        try:
            response = requests.post(
                url,
                json=payload,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()

            workflow_info = response.json()
            wid = workflow_info.get("workflow", {}).get("wid")
            logger.info(f"Workflow created successfully: {name} (wid: {wid})")
            return workflow_info

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create workflow: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise Exception(f"Failed to create workflow: {e}")

    def list_workflows(self) -> List[Dict[str, Any]]:
        """
        List all workflows accessible by the user.

        Returns:
            List of workflow information
        """
        url = f"{self.api_url}/workflow/list"

        try:
            response = requests.get(
                url,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list workflows: {e}")
            raise Exception(f"Failed to list workflows: {e}")

    def get_workflow(self, wid: int) -> Dict[str, Any]:
        """
        Get specific workflow by ID.

        Args:
            wid: Workflow ID

        Returns:
            Workflow information
        """
        url = f"{self.api_url}/workflow/{wid}"

        try:
            response = requests.get(
                url,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get workflow {wid}: {e}")
            raise Exception(f"Failed to get workflow: {e}")

    def update_workflow(
        self,
        wid: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        content: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Update an existing workflow.

        Args:
            wid: Workflow ID
            name: New workflow name (optional)
            description: New workflow description (optional)
            content: New workflow content (optional)

        Returns:
            Updated workflow information
        """
        # Get current workflow
        current = self.get_workflow(wid)

        # Update fields
        if name is not None:
            url = f"{self.api_url}/workflow/update/name"
            payload = {"wid": wid, "name": name}
            response = requests.post(
                url,
                json=payload,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Updated workflow name to: {name}")

        if description is not None:
            url = f"{self.api_url}/workflow/update/description"
            payload = {"wid": wid, "description": description}
            response = requests.post(
                url,
                json=payload,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Updated workflow description")

        if content is not None:
            # Persist workflow with new content
            url = f"{self.api_url}/workflow/persist"
            current["content"] = json.dumps(content)
            response = requests.post(
                url,
                json=current,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Updated workflow content")

        return self.get_workflow(wid)

    def delete_workflow(self, wid: int):
        """
        Delete a workflow.

        Args:
            wid: Workflow ID
        """
        url = f"{self.api_url}/workflow/delete"
        payload = {
            "wids": [wid],
            "pid": None
        }

        try:
            response = requests.post(
                url,
                json=payload,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Workflow {wid} deleted successfully")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete workflow {wid}: {e}")
            raise Exception(f"Failed to delete workflow: {e}")

    def make_workflow_public(self, wid: int):
        """
        Make a workflow public.

        Args:
            wid: Workflow ID
        """
        url = f"{self.api_url}/workflow/public/{wid}"

        try:
            response = requests.put(
                url,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Workflow {wid} is now public")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to make workflow public: {e}")
            raise Exception(f"Failed to make workflow public: {e}")

    def make_workflow_private(self, wid: int):
        """
        Make a workflow private.

        Args:
            wid: Workflow ID
        """
        url = f"{self.api_url}/workflow/private/{wid}"

        try:
            response = requests.put(
                url,
                headers=self.auth_headers,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Workflow {wid} is now private")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to make workflow private: {e}")
            raise Exception(f"Failed to make workflow private: {e}")

    def get_workflow_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get workflow information by name.

        Args:
            name: Workflow name

        Returns:
            Workflow information or None if not found
        """
        workflows = self.list_workflows()
        for wf in workflows:
            if wf.get("workflow", {}).get("name") == name:
                return wf
        return None

    def load_workflow_from_file(
        self,
        file_path: str,
        name: Optional[str] = None,
        description: str = "",
        is_public: bool = False
    ) -> Dict[str, Any]:
        """
        Load workflow from JSON file and create it.

        Args:
            file_path: Path to workflow JSON file
            name: Workflow name (defaults to filename)
            description: Workflow description
            is_public: Whether workflow is public

        Returns:
            Created workflow information

        Raises:
            Exception: If file read or workflow creation fails
        """
        if name is None:
            name = os.path.splitext(os.path.basename(file_path))[0]

        logger.info(f"Loading workflow from file: {file_path}")

        try:
            with open(file_path, 'r') as f:
                content = json.load(f)

            return self.create_workflow(
                name=name,
                description=description,
                content=content,
                is_public=is_public
            )

        except FileNotFoundError:
            logger.error(f"Workflow file not found: {file_path}")
            raise Exception(f"Workflow file not found: {file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in workflow file: {e}")
            raise Exception(f"Invalid JSON in workflow file: {e}")
