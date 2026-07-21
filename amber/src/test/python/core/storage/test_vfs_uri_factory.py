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

from core.storage.vfs_uri_factory import VFSResourceType, VFSURIFactory
from proto.org.apache.texera.amber.core import (
    ExecutionIdentity,
    GlobalPortIdentity,
    OperatorIdentity,
    PhysicalOpIdentity,
    PortIdentity,
    WorkflowIdentity,
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


class TestCreatePortBaseUri:
    def test_base_uri_encodes_scheme_ids_and_serialized_port(self):
        uri = VFSURIFactory.create_port_base_uri(
            WorkflowIdentity(id=7), ExecutionIdentity(id=3), _gpi()
        )
        # The base URI stitches together the vfs scheme, the wid/eid segments,
        # and the serialized global port identity as a single trailing segment.
        assert uri == (
            "vfs:///wid/7/eid/3/globalportid/"
            "(logicalOpId=myOp,layerName=main,portId=0,"
            "isInternal=false,isInput=true)"
        )

    def test_result_uri_appends_result_segment(self):
        base = VFSURIFactory.create_port_base_uri(
            WorkflowIdentity(id=1), ExecutionIdentity(id=1), _gpi()
        )
        assert VFSURIFactory.result_uri(base) == f"{base}/result"

    def test_state_uri_appends_state_segment(self):
        base = VFSURIFactory.create_port_base_uri(
            WorkflowIdentity(id=1), ExecutionIdentity(id=1), _gpi()
        )
        assert VFSURIFactory.state_uri(base) == f"{base}/state"


class TestDecodeUriRoundTrip:
    def test_result_uri_round_trips_through_decode(self):
        wid, eid, gpi = (
            WorkflowIdentity(id=42),
            ExecutionIdentity(id=9),
            _gpi(op_id="opA", layer="main", port=2, internal=True, is_input=False),
        )
        base = VFSURIFactory.create_port_base_uri(wid, eid, gpi)
        uri = VFSURIFactory.result_uri(base)

        decoded_wid, decoded_eid, decoded_gpi, resource_type = VFSURIFactory.decode_uri(
            uri
        )

        assert decoded_wid.id == 42
        assert decoded_eid.id == 9
        assert resource_type == VFSResourceType.RESULT
        assert decoded_gpi is not None
        assert decoded_gpi.op_id.logical_op_id.id == "opA"
        assert decoded_gpi.op_id.layer_name == "main"
        assert decoded_gpi.port_id.id == 2
        assert decoded_gpi.port_id.internal is True
        assert decoded_gpi.input is False

    def test_state_uri_round_trips_through_decode(self):
        wid, eid, gpi = WorkflowIdentity(id=5), ExecutionIdentity(id=6), _gpi()
        uri = VFSURIFactory.state_uri(VFSURIFactory.create_port_base_uri(wid, eid, gpi))

        decoded_wid, decoded_eid, decoded_gpi, resource_type = VFSURIFactory.decode_uri(
            uri
        )

        assert decoded_wid.id == 5
        assert decoded_eid.id == 6
        assert resource_type == VFSResourceType.STATE
        assert decoded_gpi.op_id.logical_op_id.id == "myOp"

    def test_decode_returns_none_port_when_globalportid_absent(self):
        # A URI without a globalportid segment yields a None port identity,
        # exercising the optional branch in decode_uri.
        wid, eid, port, resource_type = VFSURIFactory.decode_uri(
            "vfs:///wid/11/eid/22/result"
        )
        assert wid.id == 11
        assert eid.id == 22
        assert port is None
        assert resource_type == VFSResourceType.RESULT


class TestDecodeUriErrorPaths:
    def test_rejects_non_vfs_scheme(self):
        with pytest.raises(ValueError, match="Invalid URI scheme"):
            VFSURIFactory.decode_uri("http:///wid/1/eid/1/result")

    def test_rejects_missing_key_when_wid_absent(self):
        # 'wid' is never present, so extract_value fails on the index lookup.
        with pytest.raises(ValueError, match="Missing value for key: wid"):
            VFSURIFactory.decode_uri("vfs:///eid/1/result")

    def test_rejects_key_present_but_value_missing(self):
        # 'eid' is the final segment with no following value; the IndexError
        # branch of extract_value must surface as a Missing-value error.
        with pytest.raises(ValueError, match="Missing value for key: eid"):
            VFSURIFactory.decode_uri("vfs:///wid/1/eid")

    def test_rejects_unknown_resource_type(self):
        with pytest.raises(ValueError, match="Unknown resource type: bogus"):
            VFSURIFactory.decode_uri("vfs:///wid/1/eid/1/bogus")
