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

import pytest

from core.util.virtual_identity import (
    deserialize_global_port_identity,
    get_from_actor_id_for_input_port_storage,
    get_worker_index,
    serialize_global_port_identity,
)
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    GlobalPortIdentity,
    OperatorIdentity,
    PhysicalOpIdentity,
    PortIdentity,
)


def _gpi(
    op_id: str = "myOp",
    layer: str = "main",
    port: int = 0,
    internal: bool = False,
    is_input: bool = True,
) -> GlobalPortIdentity:
    return GlobalPortIdentity(
        op_id=PhysicalOpIdentity(
            logical_op_id=OperatorIdentity(id=op_id), layer_name=layer
        ),
        port_id=PortIdentity(id=port, internal=internal),
        input=is_input,
    )


class TestGetWorkerIndex:
    def test_extracts_trailing_numeric_index_from_worker_actor_name(self):
        assert get_worker_index("Worker:WF1-myOp-main-7") == 7

    def test_handles_multi_digit_indexes(self):
        assert get_worker_index("Worker:WF42-someOp-layerX-1234") == 1234

    def test_raises_value_error_on_unmatched_actor_name(self):
        # Companions like CONTROLLER / SELF do not match the worker pattern.
        with pytest.raises(ValueError, match="Invalid worker ID format"):
            get_worker_index("CONTROLLER")

    def test_raises_value_error_on_partial_match(self):
        # Missing trailing index also fails the match.
        with pytest.raises(ValueError, match="Invalid worker ID format"):
            get_worker_index("Worker:WF1-myOp-main")

    def test_extracts_trailing_index_even_when_layer_name_contains_hyphens(self):
        # The Scala VirtualIdentityUtils sibling has a documented bug where
        # the layer capture group `(\w+)` cannot accept hyphens (Bug #4728),
        # but Python's get_worker_index only consumes the trailing index
        # group `(\d+)`, so greedy backtracking on `.+` still leaves the
        # final dash-separated number for capture and the index comes out
        # correctly. Pin this so a future regex tightening that drops the
        # greedy `.+` and breaks the trailing match surfaces here.
        assert get_worker_index("Worker:WF1-myOp-1st-physical-op-3") == 3


class TestSerializeGlobalPortIdentity:
    def test_emits_documented_format_for_canonical_input(self):
        encoded = serialize_global_port_identity(_gpi())
        assert (
            encoded
            == "(logicalOpId=myOp,layerName=main,portId=0,isInternal=false,isInput=true)"
        )

    def test_lowercases_boolean_fields(self):
        # Pin: the format spec spells out `true`/`false` lowercase, even
        # though Python's str(bool) is `True`/`False`. Lowercasing is the
        # contract the deserializer relies on.
        encoded = serialize_global_port_identity(_gpi(internal=True, is_input=False))
        assert "isInternal=true" in encoded
        assert "isInput=false" in encoded

    def test_round_trips_through_deserialize(self):
        original = _gpi(
            op_id="myOp", layer="main", port=3, internal=True, is_input=False
        )
        recovered = deserialize_global_port_identity(
            serialize_global_port_identity(original)
        )
        assert recovered.op_id.logical_op_id.id == "myOp"
        assert recovered.op_id.layer_name == "main"
        assert recovered.port_id.id == 3
        assert recovered.port_id.internal is True
        assert recovered.input is False


class TestDeserializeGlobalPortIdentity:
    def test_parses_canonical_encoded_string(self):
        encoded = "(logicalOpId=op,layerName=l,portId=2,isInternal=true,isInput=true)"
        result = deserialize_global_port_identity(encoded)
        assert result.op_id.logical_op_id.id == "op"
        assert result.op_id.layer_name == "l"
        assert result.port_id.id == 2
        assert result.port_id.internal is True
        assert result.input is True

    def test_treats_boolean_capitalization_case_insensitively(self):
        # The deserializer lowercases the captured token before comparing,
        # so producers that emit `True`/`TRUE` still parse cleanly even
        # though the canonical serializer always writes lowercase.
        encoded = "(logicalOpId=op,layerName=l,portId=0,isInternal=TRUE,isInput=False)"
        result = deserialize_global_port_identity(encoded)
        assert result.port_id.internal is True
        assert result.input is False

    def test_raises_value_error_on_malformed_input(self):
        with pytest.raises(ValueError, match="Invalid GlobalPortIdentity format"):
            deserialize_global_port_identity("not-a-port-id")

    def test_raises_value_error_on_missing_field(self):
        # The pattern requires all five comma-separated fields. Dropping one
        # — here `isInput` — must surface as ValueError, not silent default.
        with pytest.raises(ValueError, match="Invalid GlobalPortIdentity format"):
            deserialize_global_port_identity(
                "(logicalOpId=op,layerName=l,portId=0,isInternal=true)"
            )


class TestGetFromActorIdForInputPortStorage:
    def test_prefixes_materialization_reader_to_uri_plus_actor_name(self):
        actor = ActorVirtualIdentity(name="Worker:WF1-myOp-main-0")
        virtual_reader = get_from_actor_id_for_input_port_storage(
            "iceberg:/warehouse/x", actor
        )
        assert virtual_reader.name == (
            "MATERIALIZATION_READER_iceberg:/warehouse/xWorker:WF1-myOp-main-0"
        )
