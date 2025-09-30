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

from enum import Enum
from typing import Optional
from urllib.parse import urlparse

from core.util.virtual_identity import (
    serialize_global_port_identity,
    deserialize_global_port_identity,
)
from proto.edu.uci.ics.amber.core import (
    WorkflowIdentity,
    ExecutionIdentity,
    GlobalPortIdentity,
)


class VFSResourceType(str, Enum):
    RESULT = "result"
    RUNTIME_STATISTICS = "runtimeStatistics"
    CONSOLE_MESSAGES = "consoleMessages"


class VFSURIFactory:
    VFS_FILE_URI_SCHEME = "vfs"

    @staticmethod
    def decode_uri(
        uri: str,
    ) -> (
        WorkflowIdentity,
        ExecutionIdentity,
        Optional[GlobalPortIdentity],
        VFSResourceType,
    ):
        """
        Parses a VFS URI and extracts its components.
        """
        parsed_uri = urlparse(uri)

        if parsed_uri.scheme != VFSURIFactory.VFS_FILE_URI_SCHEME:
            raise ValueError(f"Invalid URI scheme: {parsed_uri.scheme}")

        segments = parsed_uri.path.lstrip("/").split("/")

        def extract_value(key: str) -> str:
            try:
                index = segments.index(key)
                return segments[index + 1]
            except (ValueError, IndexError):
                raise ValueError(f"Missing value for key: {key} in URI: {uri}")

        workflow_id = WorkflowIdentity(int(extract_value("wid")))
        execution_id = ExecutionIdentity(int(extract_value("eid")))

        global_port_id = (
            deserialize_global_port_identity(extract_value("globalportid"))
            if "globalportid" in segments
            else None
        )

        resource_type_str = segments[-1].lower()
        try:
            resource_type = VFSResourceType(resource_type_str)
        except ValueError:
            raise ValueError(f"Unknown resource type: {resource_type_str}")

        return (
            workflow_id,
            execution_id,
            global_port_id,
            resource_type,
        )

    @staticmethod
    def create_result_uri(workflow_id, execution_id, global_port_id) -> str:
        """Creates a URI pointing to a result storage."""
        base_uri = (
            f"{VFSURIFactory.VFS_FILE_URI_SCHEME}:///wid/{workflow_id.id}"
            f"/eid/{execution_id.id}/globalportid/"
            f"{serialize_global_port_identity(global_port_id)}"
        )

        return f"{base_uri}/{VFSResourceType.RESULT.value}"
