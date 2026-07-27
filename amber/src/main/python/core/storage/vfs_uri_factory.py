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
from urllib.parse import unquote, urlparse

from core.util.virtual_identity import (
    serialize_global_port_identity,
    deserialize_global_port_identity,
)
from proto.org.apache.texera.amber.core import (
    WorkflowIdentity,
    ExecutionIdentity,
    GlobalPortIdentity,
)


class VFSResourceType(str, Enum):
    RESULT = "result"
    RUNTIME_STATISTICS = "runtimeStatistics"
    CONSOLE_MESSAGES = "consoleMessages"
    STATE = "state"


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
    def warehouse_from_uri(uri: str) -> Optional[str]:
        """
        Extracts the warehouse name from the optional leading "/wh/<name>" segment
        of a VFS URI, or None if absent. Mirrors VFSURIFactory.warehouseFromURI
        (Scala) so a URI fully identifies which warehouse its tables live in.

        Anchored to the leading segment, matching Scala and the leading-only strip
        in document_factory.sanitize_uri_path: a later segment that happens to be
        "wh" must not select a warehouse. The path is unquoted first because
        java.net.URI.getPath decodes on the Scala side while urlparse does not --
        without it the two languages disagree on a percent-encoded name.
        """
        segments = unquote(urlparse(uri).path).lstrip("/").split("/")
        if len(segments) >= 2 and segments[0] == "wh" and segments[1]:
            return segments[1]
        return None

    @staticmethod
    def create_port_base_uri(
        workflow_id, execution_id, global_port_id, warehouse: Optional[str] = None
    ) -> str:
        """Base URI for a port. Result and state URIs derive from it via
        `result_uri` / `state_uri`.

        `warehouse` is written as the leading "/wh/<name>" segment, mirroring the
        Scala side; when None the URI is byte-for-byte what it was before warehouses
        existed.
        """
        wh_segment = f"/wh/{warehouse}" if warehouse else ""
        return (
            f"{VFSURIFactory.VFS_FILE_URI_SCHEME}://{wh_segment}/wid/{workflow_id.id}"
            f"/eid/{execution_id.id}/globalportid/"
            f"{serialize_global_port_identity(global_port_id)}"
        )

    @staticmethod
    def result_uri(base_uri: str) -> str:
        """The result-resource URI under a port base URI."""
        return f"{base_uri}/{VFSResourceType.RESULT.value}"

    @staticmethod
    def state_uri(base_uri: str) -> str:
        """The state-resource URI under a port base URI."""
        return f"{base_uri}/{VFSResourceType.STATE.value}"
