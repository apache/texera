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

import pickle

import cloudpickle
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


@pytest.mark.parametrize("identity", [None, 1, True, b"edge", []])
def test_identities_require_strings(identity):
    with pytest.raises(TypeError, match="string"):
        BoundaryPayload(identity, (), (), b"")
    with pytest.raises(TypeError, match="string"):
        WorkflowEnvelope(identity, ())


@pytest.mark.parametrize("identity", ["", "\u96ea"])
def test_empty_and_unicode_identities(identity):
    if not identity:
        with pytest.raises(ValueError, match="nonempty"):
            WorkflowEnvelope(identity, ())
        with pytest.raises(ValueError, match="nonempty"):
            BoundaryPayload(identity, (), (), b"")
    else:
        envelope = WorkflowEnvelope(identity, (encode_boundary(identity, (), ()),))
        assert loads_envelope(dumps_envelope(envelope)) == envelope


@pytest.mark.parametrize(
    ("target", "field", "value", "error"),
    [
        ("envelope", "execution_key", 42, TypeError),
        ("envelope", "execution_key", "", ValueError),
        ("envelope", "boundaries", [], TypeError),
        ("boundary", "boundary_id", 42, TypeError),
        ("boundary", "fields", ["value"], TypeError),
        ("boundary", "fields", (1,), TypeError),
        ("boundary", "present", ["value"], TypeError),
        ("boundary", "present", ("other",), ValueError),
        ("boundary", "payload", "bytes", TypeError),
    ],
)
def test_envelope_codec_revalidates_unpickled_metadata(target, field, value, error):
    boundary = encode_boundary("edge", ("value",), (42,))
    envelope = WorkflowEnvelope("run", (boundary,))
    object.__setattr__(envelope if target == "envelope" else boundary, field, value)
    # Simulate an existing serialized instance: pickle bypasses constructors.
    payload = cloudpickle.dumps(envelope)
    with pytest.raises(error):
        loads_envelope(payload)
    with pytest.raises(error):
        dumps_envelope(envelope)


def test_merge_rejects_overlapping_ids_and_orders_disjoint_ids():
    first = encode_boundary("z", ("value",), (1,))
    second = encode_boundary("z", ("value",), (2,))
    with pytest.raises(ValueError, match="unique"):
        merge_envelopes(
            WorkflowEnvelope("run", (first,)), WorkflowEnvelope("run", (second,))
        )
    other = encode_boundary("a", ("value",), (3,))
    merged = merge_envelopes(
        WorkflowEnvelope("run", (first,)), WorkflowEnvelope("run", (other,))
    )
    assert merged.boundaries == (other, first)
    assert decode_boundary(merged.boundaries[0], ("value",)) == (3,)
    assert decode_boundary(merged.boundaries[1], ("value",)) == (1,)
    with pytest.raises(TypeError):
        _ = first < other


@pytest.mark.parametrize("values", [[42], {"value": 42}, "x", None])
def test_encoder_rejects_non_tuple_values_before_serialization(values, monkeypatch):
    def unexpected_serialization(*args, **kwargs):
        pytest.fail("invalid values reached serialization")

    monkeypatch.setattr(cloudpickle, "dumps", unexpected_serialization)
    with pytest.raises(TypeError, match="tuple"):
        encode_boundary("edge", ("value",), values)


@pytest.mark.parametrize(
    ("payload", "error"), [(b"", EOFError), (b"!", pickle.UnpicklingError)]
)
def test_decoders_preserve_pickle_exceptions(payload, error):
    with pytest.raises(error):
        loads_envelope(payload)
    with pytest.raises(error):
        decode_boundary(BoundaryPayload("edge", (), (), payload), ())
