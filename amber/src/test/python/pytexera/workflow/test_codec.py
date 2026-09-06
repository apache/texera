# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
from pytexera.workflow.codec import (
    BoundaryPayload,
    WorkflowEnvelope,
    decode_boundary,
    dumps_envelope,
    encode_boundary,
    loads_envelope,
    merge_envelopes,
)


def test_boundary_cloudpickle_preserves_aliases_and_cycles() -> None:
    shared = []
    shared.append(shared)

    boundary = encode_boundary("edge", ("left", "right"), (shared, shared))
    values = decode_boundary(boundary, ("left", "right"))

    assert values[0] is values[1]
    assert values[0][0] is values[0]


def test_envelope_round_trip_contains_only_explicit_boundaries() -> None:
    boundary = encode_boundary("edge", ("value",), ({"large": [1, 2, 3]},))
    envelope = WorkflowEnvelope("run-1", (boundary,))

    decoded = loads_envelope(dumps_envelope(envelope))

    assert decoded == envelope
    assert decoded.boundaries[0].fields == ("value",)


def test_decode_rejects_field_contract_mismatch() -> None:
    boundary = encode_boundary("edge", ("value",), (1,))

    with pytest.raises(ValueError, match="fields"):
        decode_boundary(boundary, ("other",))


def test_boundary_cloudpickle_preserves_an_absent_selected_field() -> None:
    """The wire contract and the values present on one path remain distinct."""

    boundary = encode_boundary(
        "edge",
        ("left", "right"),
        (41,),
        present=("left",),
    )

    assert boundary.fields == ("left", "right")
    assert boundary.present == ("left",)
    assert decode_boundary(boundary, ("left", "right")) == (41,)


def test_envelope_rejects_duplicate_boundaries_and_cross_key_merge() -> None:
    boundary = BoundaryPayload("edge", ("value",), ("value",), b"payload")
    with pytest.raises(ValueError, match="canonical"):
        WorkflowEnvelope("run", (boundary, boundary))

    with pytest.raises(ValueError, match="execution key"):
        merge_envelopes(
            WorkflowEnvelope("left", ()),
            WorkflowEnvelope("right", ()),
        )
