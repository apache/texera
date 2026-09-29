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
from typing import NamedTuple, Optional
import re
from urllib.parse import urlparse

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


# See VFSURIFactory._is_valid_warehouse_name.
_WAREHOUSE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")


class VFSUriComponents(NamedTuple):
    """The named components encoded in a VFS URI, as returned by
    `VFSURIFactory.decode_uri`. A NamedTuple, so positional unpacking keeps
    working alongside named access."""

    workflow_id: WorkflowIdentity
    execution_id: ExecutionIdentity
    global_port_id: Optional[GlobalPortIdentity]
    resource_type: VFSResourceType
    # The warehouse whose catalog holds this URI's table, from the optional leading
    # "/wh/<name>" segment; None for non-BYO storage, which uses the configured
    # default. Last so positional unpacking of the earlier fields still works.
    warehouse: Optional[str] = None


class VFSURIFactory:
    VFS_FILE_URI_SCHEME = "vfs"

    @staticmethod
    def decode_uri(uri: str) -> "VFSUriComponents":
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

        resource_type_str = segments[-1]
        try:
            resource_type = VFSResourceType(resource_type_str)
        except ValueError:
            raise ValueError(f"Unknown resource type: {resource_type_str}")

        return VFSUriComponents(
            workflow_id,
            execution_id,
            global_port_id,
            resource_type,
            VFSURIFactory._warehouse_from(segments),
        )

    @staticmethod
    def _is_valid_warehouse_name(name: str) -> bool:
        """A warehouse name becomes a URI path segment, so it may not carry
        characters that have meaning there: no "/" to add segments, and no "%" to
        smuggle one in percent-encoded form. Mirrors VFSURIFactory (Scala).
        """
        return _WAREHOUSE_NAME_RE.fullmatch(name) is not None

    @staticmethod
    def _warehouse_from(segments: list) -> Optional[str]:
        """
        The warehouse encoded in a VFS URI, if present. Reported as part of
        VFSUriComponents by decode_uri, which is the only way in. Mirrors
        VFSURIFactory.warehouseFrom (Scala).

        Anchored to the leading segment: a later segment that happens to be "wh"
        -- e.g. inside a user-chosen operator id -- must not select a warehouse; it
        would disagree with document_factory.sanitize_uri_path, which strips only a
        leading one, and would route the write to another user's warehouse. The
        segments come from the RAW path (see decode_uri), so a percent-encoded slash
        stays inside its own segment, and the name must be a legal warehouse name,
        so anything create_port_base_uri could not have written resolves to no
        warehouse rather than to a wrong one.
        """
        if (
            len(segments) >= 2
            and segments[0] == "wh"
            and VFSURIFactory._is_valid_warehouse_name(segments[1])
        ):
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
        if warehouse is not None and not VFSURIFactory._is_valid_warehouse_name(
            warehouse
        ):
            raise ValueError(
                f"warehouse name must match {_WAREHOUSE_NAME_RE.pattern} "
                f"(it becomes a URI path segment): {warehouse}"
            )
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
