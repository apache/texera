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

from core.models.state import State


class TestState:
    def test_state_subclasses_dict(self):
        state = State({"a": 1})
        assert isinstance(state, dict)
        assert state["a"] == 1
        assert State() == {}

    def test_class_attributes(self):
        assert State.CONTENT == "content"
        assert State.LOOP_COUNTER == "loop_counter"
        assert State.LOOP_START_ID == "loop_start_id"
        # The loop-control columns are runtime-owned bookkeeping, sibling to
        # content, not part of the user state JSON.
        assert State.SCHEMA.get_attr_names() == [
            "content",
            "loop_counter",
            "loop_start_id",
        ]

    def test_json_round_trip_primitives(self):
        original = State(
            {
                "string": "hello",
                "int": 42,
                "float": 3.14,
                "bool_true": True,
                "bool_false": False,
                "none_value": None,
            }
        )
        decoded = State.from_json(original.to_json())
        assert decoded == original

    def test_json_round_trip_empty(self):
        assert State.from_json(State().to_json()) == State()

    def test_json_round_trip_bytes(self):
        original = State({"payload": b"\x00\x01\x02\xff"})
        decoded = State.from_json(original.to_json())
        assert decoded["payload"] == b"\x00\x01\x02\xff"
        assert isinstance(decoded["payload"], bytes)

    def test_json_round_trip_nested_dict(self):
        original = State({"outer": {"inner": {"value": 1}}})
        decoded = State.from_json(original.to_json())
        assert decoded == original

    def test_json_round_trip_list_of_mixed_values(self):
        original = State({"items": [1, "two", 3.0, True, None]})
        decoded = State.from_json(original.to_json())
        assert decoded == original

    def test_json_round_trip_bytes_inside_list_and_nested_dict(self):
        original = State(
            {
                "blobs": [b"first", b"second"],
                "nested": {"sub_blob": b"inside"},
            }
        )
        decoded = State.from_json(original.to_json())
        assert decoded["blobs"] == [b"first", b"second"]
        assert decoded["nested"]["sub_blob"] == b"inside"

    def test_to_json_rejects_non_serializable_value(self):
        class Custom:
            pass

        with pytest.raises(TypeError):
            State({"bad": Custom()}).to_json()

    def test_tuple_round_trip(self):
        original = State({"i": 3, "label": "outer", "blob": b"\x01\x02"})
        decoded = State.from_tuple(original.to_tuple())
        assert decoded == original

    def test_to_tuple_writes_content_and_loop_counter_columns(self):
        tuple_ = State({"x": 1}).to_tuple(7)
        # content holds the JSON serialization; loop_counter is its own column.
        assert tuple_[State.CONTENT] == '{"x":1}'
        assert tuple_[State.LOOP_COUNTER] == 7
        # loop_counter must not leak into the content JSON.
        assert "loop_counter" not in tuple_[State.CONTENT]

    def test_to_tuple_defaults_loop_counter_to_zero(self):
        assert State({"x": 1}).to_tuple()[State.LOOP_COUNTER] == 0

    def test_nested_dict_decodes_to_plain_dict(self):
        # Top-level returns a State; nested dicts come back as plain dict.
        # This is intentional -- only the outermost mapping is wrapped.
        decoded = State.from_json('{"outer":{"inner":1}}')
        assert isinstance(decoded, State)
        assert isinstance(decoded["outer"], dict)
        assert not isinstance(decoded["outer"], State)
