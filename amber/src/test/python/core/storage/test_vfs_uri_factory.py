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
    @pytest.mark.parametrize(
        ("suffix", "resource_type"),
        [
            ("runtimeStatistics", VFSResourceType.RUNTIME_STATISTICS),
            ("consoleMessages", VFSResourceType.CONSOLE_MESSAGES),
        ],
    )
    def test_decode_accepts_camel_case_resource_types(self, suffix, resource_type):
        components = VFSURIFactory.decode_uri(f"vfs:///wid/1/eid/2/{suffix}")
        assert components.resource_type == resource_type

    def test_result_uri_round_trips_through_decode(self):
        wid, eid, gpi = (
            WorkflowIdentity(id=42),
            ExecutionIdentity(id=9),
            _gpi(op_id="opA", layer="main", port=2, internal=True, is_input=False),
        )
        base = VFSURIFactory.create_port_base_uri(wid, eid, gpi)
        uri = VFSURIFactory.result_uri(base)

        components = VFSURIFactory.decode_uri(uri)

        assert components.workflow_id.id == 42
        assert components.execution_id.id == 9
        assert components.resource_type == VFSResourceType.RESULT
        decoded_gpi = components.global_port_id
        assert decoded_gpi is not None
        assert decoded_gpi.op_id.logical_op_id.id == "opA"
        assert decoded_gpi.op_id.layer_name == "main"
        assert decoded_gpi.port_id.id == 2
        assert decoded_gpi.port_id.internal is True
        assert decoded_gpi.input is False

    def test_state_uri_round_trips_through_decode(self):
        wid, eid, gpi = WorkflowIdentity(id=5), ExecutionIdentity(id=6), _gpi()
        uri = VFSURIFactory.state_uri(VFSURIFactory.create_port_base_uri(wid, eid, gpi))

        components = VFSURIFactory.decode_uri(uri)

        assert components.workflow_id.id == 5
        assert components.execution_id.id == 6
        assert components.resource_type == VFSResourceType.STATE
        assert components.global_port_id.op_id.logical_op_id.id == "myOp"

    def test_decode_returns_none_port_when_globalportid_absent(self):
        # A URI without a globalportid segment yields a None port identity,
        # exercising the optional branch in decode_uri.
        components = VFSURIFactory.decode_uri("vfs:///wid/11/eid/22/result")
        assert components.workflow_id.id == 11
        assert components.execution_id.id == 22
        assert components.global_port_id is None
        assert components.resource_type == VFSResourceType.RESULT


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


class TestWarehouseInDecodedUri:
    def test_reports_warehouse_from_wh_segment(self):
        components = VFSURIFactory.decode_uri("vfs:///wh/user-2-foo/wid/7/eid/3/result")
        assert components.warehouse == "user-2-foo"

    def test_warehouse_is_none_when_no_wh_segment(self):
        # Non-BYO URIs have no /wh/ segment, so the warehouse is absent.
        assert VFSURIFactory.decode_uri("vfs:///wid/7/eid/3/result").warehouse is None

    def test_only_a_leading_wh_segment_counts(self):
        # A `wh` deeper in the path -- e.g. inside an operator id -- must not select
        # a warehouse; it would disagree with sanitize_uri_path, which strips only a
        # leading one, and would route the write to another user's warehouse.
        assert (
            VFSURIFactory.decode_uri(
                "vfs:///wid/1/eid/2/opid/a/wh/victim/b/result"
            ).warehouse
            is None
        )
        assert (
            VFSURIFactory.decode_uri("vfs:///wid/1/eid/2/opid/wh/result").warehouse
            is None
        )

    def test_percent_encoded_name_is_rejected_not_decoded(self):
        # The raw path is split and the name must be a legal warehouse name, so a
        # percent-encoded name resolves to no warehouse rather than to a decoded
        # one. Decoding first would let "%2F" become a separator and pick the wrong
        # warehouse; it would also diverge from Scala, which splits its raw path.
        assert (
            VFSURIFactory.decode_uri(
                "vfs:///wh/user-2%2Dfoo/wid/7/eid/3/result"
            ).warehouse
            is None
        )

    def test_encoded_slash_in_name_cannot_forge_segments(self):
        # "%2F" must stay inside its own segment: decoded-then-split, this URI would
        # read as /wh/a/wid/999/... handing back "a" as the warehouse, and the key
        # search would find the injected `wid` instead of the real one.
        components = VFSURIFactory.decode_uri(
            "vfs:///wh/a%2Fwid%2F999/wid/1/eid/2/result"
        )
        assert components.warehouse is None
        assert components.workflow_id.id == 1
        assert components.execution_id.id == 2

    def test_builder_rejects_an_unsafe_warehouse_name(self):
        with pytest.raises(ValueError, match="warehouse name must match"):
            VFSURIFactory.create_port_base_uri(
                WorkflowIdentity(id=7), ExecutionIdentity(id=3), _gpi(), "a/b"
            )

    def test_round_trips_a_warehouse_written_by_the_python_builder(self):
        # Python can now write the segment, not just read it.
        uri = VFSURIFactory.create_port_base_uri(
            WorkflowIdentity(id=7), ExecutionIdentity(id=3), _gpi(), "user-2-foo"
        )
        assert uri.startswith("vfs:///wh/user-2-foo/wid/7/eid/3/")
        assert (
            VFSURIFactory.decode_uri(VFSURIFactory.result_uri(uri)).warehouse
            == "user-2-foo"
        )

    def test_builder_without_warehouse_is_unchanged(self):
        uri = VFSURIFactory.create_port_base_uri(
            WorkflowIdentity(id=7), ExecutionIdentity(id=3), _gpi()
        )
        assert "/wh/" not in uri
        assert VFSURIFactory.decode_uri(VFSURIFactory.result_uri(uri)).warehouse is None

    def test_decode_round_trips_a_warehouse_scoped_uri(self):
        # The leading /wh/<name> segment must not break decode_uri, which finds
        # wid/eid by key rather than by position.
        components = VFSURIFactory.decode_uri(
            "vfs:///wh/user-2-foo/wid/11/eid/22/result"
        )
        assert components.workflow_id.id == 11
        assert components.execution_id.id == 22
        assert components.global_port_id is None
        assert components.resource_type == VFSResourceType.RESULT
        assert components.warehouse == "user-2-foo"
