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

import re
from proto.edu.uci.ics.amber.core import (
    GlobalPortIdentity,
    PhysicalOpIdentity,
    OperatorIdentity,
    PortIdentity,
    ActorVirtualIdentity,
)

worker_name_pattern = re.compile(r"Worker:WF\d+-.+-(\w+)-(\d+)")

MATERIALIZATION_READER_ACTOR_PREFIX = "MATERIALIZATION_READER_"


def get_worker_index(worker_id: str) -> int:
    match = worker_name_pattern.match(worker_id)
    if match:
        return int(match.group(2))
    raise ValueError("Invalid worker ID format")


def serialize_global_port_identity(obj: GlobalPortIdentity) -> str:
    """
    Serialize GlobalPortIdentity into a custom human-readable string.
    Expected format:
    ``(logicalOpId=<logicalOpId>,layerName=<layerName>,
    portId=<portId.id>,isInternal=<portId.internal>,isInput=<input>)``
    """
    logical_op_id = obj.op_id.logical_op_id.id
    layer_name = obj.op_id.layer_name
    port_id = obj.port_id.id
    is_internal = obj.port_id.internal
    is_input_port = obj.input
    return (
        f"(logicalOpId={logical_op_id},layerName={layer_name},portId={port_id},"
        f"isInternal={str(is_internal).lower()},isInput={str(is_input_port).lower()})"
    )


def deserialize_global_port_identity(encoded_str: str) -> GlobalPortIdentity:
    """
    Deserialize a custom string from the format
    ``(logicalOpId=<logicalOpId>,layerName=<layerName>,
    portId=<portId.id>,isInternal=<portId.internal>,isInput=<input>)``
    back into a GlobalPortIdentity object.
    """
    pattern = (
        r"\(logicalOpId=([^,]+),layerName=([^,]+),"
        r"portId=([^,]+),isInternal=([^,]+),isInput=([^)]+)\)"
    )
    match = re.fullmatch(pattern, encoded_str)
    if not match:
        raise ValueError(f"Invalid GlobalPortIdentity format: {encoded_str}")
    logical_op_id, layer_name, port_id_str, is_internal_str, is_input_str = (
        match.groups()
    )
    port_id = int(port_id_str)
    is_internal = is_internal_str.lower() == "true"
    is_input_port = is_input_str.lower() == "true"
    op_id = PhysicalOpIdentity(
        logical_op_id=OperatorIdentity(id=logical_op_id), layer_name=layer_name
    )
    port = PortIdentity(id=port_id, internal=is_internal)
    return GlobalPortIdentity(op_id=op_id, port_id=port, input=is_input_port)


def get_from_actor_id_for_input_port_storage(
    storage_uri_str: str, to_worker_actor_id: ActorVirtualIdentity
) -> ActorVirtualIdentity:
    """
    Constructs an ActorVirtualIdentity for input port storage.

    Args:
        storage_uri_str (str): The string representation of the storage URI.

    Returns:
        ActorVirtualIdentity: A new virtual identity created by
        prefixing the storage URI.
    """
    return ActorVirtualIdentity(
        MATERIALIZATION_READER_ACTOR_PREFIX + storage_uri_str + to_worker_actor_id.name
    )
