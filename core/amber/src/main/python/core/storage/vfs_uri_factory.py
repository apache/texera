from enum import Enum
from typing import Optional
from urllib.parse import urlparse

from proto.edu.uci.ics.amber.core import (
    WorkflowIdentity,
    ExecutionIdentity,
    OperatorIdentity,
    PortIdentity,
)


class VFSResourceType(str, Enum):
    RESULT = "result"
    MATERIALIZED_RESULT = "materialized_result"


class VFSURIFactory:
    VFS_FILE_URI_SCHEME = "vfs"

    @staticmethod
    def decode_uri(
        uri: str,
    ) -> (
        WorkflowIdentity,
        ExecutionIdentity,
        OperatorIdentity,
        Optional[PortIdentity],
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
        operator_id = OperatorIdentity(extract_value("opid"))

        port_identity = VFSURIFactory._extract_optional_port_identity(segments, uri)

        resource_type_str = segments[-1].lower()
        try:
            resource_type = VFSResourceType(resource_type_str)
        except ValueError:
            raise ValueError(f"Unknown resource type: {resource_type_str}")

        return workflow_id, execution_id, operator_id, port_identity, resource_type

    @staticmethod
    def _extract_optional_port_identity(segments, uri):
        if "pid" in segments:
            try:
                pid_index = segments.index("pid")
                port_id_str, port_type = segments[pid_index + 1].split("_")
                port_id = int(port_id_str)
                if port_type != "I" and port_type != "E":
                    raise ValueError(f"Invalid port type: {port_type} in URI: {uri}")
                is_internal = port_type == "I"
                return PortIdentity(port_id, is_internal)
            except (ValueError, IndexError):
                raise ValueError(f"Invalid port information in URI: {uri}")
        else:
            return None

    @staticmethod
    def create_result_uri(workflow_id, execution_id, operator_id, port_identity) -> str:
        """Creates a URI pointing to a result storage."""
        return VFSURIFactory._create_vfs_uri(
            VFSResourceType.RESULT,
            workflow_id,
            execution_id,
            operator_id,
            port_identity,
        )

    @staticmethod
    def create_materialized_result_uri(
        workflow_id, execution_id, operator_id, port_identity
    ) -> str:
        """Creates a URI pointing to a materialized storage."""
        return VFSURIFactory._create_vfs_uri(
            VFSResourceType.MATERIALIZED_RESULT,
            workflow_id,
            execution_id,
            operator_id,
            port_identity,
        )

    @staticmethod
    def _create_vfs_uri(
        resource_type, workflow_id, execution_id, operator_id, port_identity=None
    ) -> str:
        """Internal helper to create URI pointing to a VFS resource."""
        if (
            resource_type
            in (VFSResourceType.RESULT, VFSResourceType.MATERIALIZED_RESULT)
            and port_identity is None
        ):
            raise ValueError(
                "PortIdentity must be provided when resourceType is RESULT or "
                "MATERIALIZED_RESULT."
            )

        base_uri = (
            f"{VFSURIFactory.VFS_FILE_URI_SCHEME}:///wid/{workflow_id.id}"
            f"/eid/{execution_id.id}/opid/{operator_id.id}"
        )

        if port_identity is not None:
            port_type = "I" if port_identity.internal else "E"
            base_uri += f"/pid/{port_identity.id}_{port_type}"

        return f"{base_uri}/{resource_type.value}"
