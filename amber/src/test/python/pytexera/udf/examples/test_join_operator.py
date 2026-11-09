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

from pytexera import Tuple
from pytexera.udf.examples.join_operator import JoinOperator


class TestJoinOperator:
    @pytest.fixture
    def join_operator(self):
        operator = JoinOperator()
        operator.open()
        return operator

    def test_match_emits_one_merged_tuple(self, join_operator):
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "alice"}), 0))

        outputs = join_operator.process_tuple(Tuple({"key": 1, "score": 95}), 1)
        output = next(outputs)

        assert dict(output) == {"key": 1, "name": "alice", "score": 95}
        with pytest.raises(StopIteration):
            next(outputs)

    def test_no_matching_key_emits_nothing(self, join_operator):
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "alice"}), 0))

        outputs = list(join_operator.process_tuple(Tuple({"key": 2, "score": 95}), 1))

        assert outputs == []

    def test_duplicate_build_keys_emit_one_output_each(self, join_operator):
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "alice"}), 0))
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "bob"}), 0))

        outputs = list(join_operator.process_tuple(Tuple({"key": 1, "score": 95}), 1))

        assert [dict(output) for output in outputs] == [
            {"key": 1, "name": "alice", "score": 95},
            {"key": 1, "name": "bob", "score": 95},
        ]

    def test_build_side_emits_nothing(self, join_operator):
        outputs = list(
            join_operator.process_tuple(Tuple({"key": 1, "name": "alice"}), 0)
        )

        assert outputs == []

    def test_probe_on_empty_build_side_emits_nothing(self, join_operator):
        outputs = list(join_operator.process_tuple(Tuple({"key": 1, "score": 95}), 1))

        assert outputs == []

    def test_colliding_column_takes_probe_side_value(self, join_operator):
        # Both sides carry a "note" column; the probe (right) side wins.
        list(join_operator.process_tuple(Tuple({"key": 1, "note": "from-left"}), 0))

        outputs = list(
            join_operator.process_tuple(Tuple({"key": 1, "note": "from-right"}), 1)
        )

        assert [dict(output) for output in outputs] == [
            {"key": 1, "note": "from-right"}
        ]

    def test_none_keys_join_each_other(self, join_operator):
        # Unlike SQL NULL semantics, None is an ordinary hashable dict key in
        # Python, so None keys join each other; this pins the current behavior.
        list(join_operator.process_tuple(Tuple({"key": None, "name": "l"}), 0))

        outputs = list(join_operator.process_tuple(Tuple({"key": None, "score": 1}), 1))

        assert [dict(output) for output in outputs] == [
            {"key": None, "name": "l", "score": 1}
        ]

    def test_merged_output_column_order(self, join_operator):
        # Left columns keep their order and position (including collided
        # ones), probe-only columns are appended, and collided values come
        # from the probe side.
        list(join_operator.process_tuple(Tuple({"key": 1, "a": "x", "note": "L"}), 0))

        outputs = list(
            join_operator.process_tuple(Tuple({"key": 1, "note": "R", "b": "y"}), 1)
        )

        assert len(outputs) == 1
        assert list(outputs[0].keys()) == ["key", "a", "note", "b"]
        assert outputs[0]["note"] == "R"

    def test_two_by_two_match_emits_four_outputs(self, join_operator):
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "alice"}), 0))
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "bob"}), 0))

        outputs = list(join_operator.process_tuple(Tuple({"key": 1, "score": 95}), 1))
        outputs += list(join_operator.process_tuple(Tuple({"key": 1, "score": 96}), 1))

        assert [dict(output) for output in outputs] == [
            {"key": 1, "name": "alice", "score": 95},
            {"key": 1, "name": "bob", "score": 95},
            {"key": 1, "name": "alice", "score": 96},
            {"key": 1, "name": "bob", "score": 96},
        ]

    def test_emitted_output_does_not_alias_stored_left_tuple(self, join_operator):
        # Mutating an emitted output must not corrupt the stored build side.
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "alice"}), 0))

        first_outputs = list(
            join_operator.process_tuple(Tuple({"key": 1, "score": 95}), 1)
        )
        first_outputs[0]["name"] = "MUTATED"
        second_outputs = list(
            join_operator.process_tuple(Tuple({"key": 1, "score": 96}), 1)
        )

        assert [dict(output) for output in second_outputs] == [
            {"key": 1, "name": "alice", "score": 96}
        ]

    def test_int_key_matches_equal_float_key(self, join_operator):
        # Python hashes/compares equal numeric values across types, so the
        # two ports' key columns may legitimately differ in type.
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "alice"}), 0))

        outputs = list(join_operator.process_tuple(Tuple({"key": 1.0, "score": 95}), 1))

        assert [dict(output) for output in outputs] == [
            {"key": 1.0, "name": "alice", "score": 95}
        ]

    def test_int_key_does_not_match_string_key(self, join_operator):
        list(join_operator.process_tuple(Tuple({"key": 1, "name": "alice"}), 0))

        outputs = list(join_operator.process_tuple(Tuple({"key": "1", "score": 95}), 1))

        assert outputs == []
