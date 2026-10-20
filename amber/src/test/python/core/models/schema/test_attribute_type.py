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

import datetime

import pytest

from core.models.schema.attribute_type import (
    AttributeType,
    FROM_STRING_PARSER_MAPPING,
)

# Go through the dispatch table rather than the private helpers: this is the
# entry point production code uses when it materializes a string column into
# a typed field, so the tests pin the reachable behaviour rather than an
# implementation detail.
parse_bool = FROM_STRING_PARSER_MAPPING[AttributeType.BOOL]
parse_timestamp = FROM_STRING_PARSER_MAPPING[AttributeType.TIMESTAMP]

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


class TestParseBool:
    @pytest.mark.parametrize("empty", [None, "", "   ", "\t\n"])
    def test_an_absent_value_parses_as_false(self, empty):
        # Every "empty" spelling short-circuits to False before the
        # true/false/numeric ladder is consulted. Covering several spellings
        # keeps the assertion from being dodged by a narrower emptiness test.
        assert parse_bool(empty) is False

    @pytest.mark.parametrize(
        "text, expected",
        [("true", True), ("TRUE", True), (" True ", True), ("false", False)],
    )
    def test_literal_spellings_are_case_and_space_insensitive(self, text, expected):
        assert parse_bool(text) is expected

    @pytest.mark.parametrize("text, expected", [("1", True), ("0", False)])
    def test_a_numeric_value_is_compared_against_zero(self, text, expected):
        assert parse_bool(text) is expected

    def test_a_non_numeric_non_literal_value_is_rejected(self):
        with pytest.raises(ValueError):
            parse_bool("maybe")


class TestParseTimestamp:
    @pytest.mark.parametrize("empty", [None, "", "   ", "\t\n"])
    def test_an_absent_value_parses_as_the_utc_epoch(self, empty):
        parsed = parse_timestamp(empty)
        # Assert the exact instant *and* the tzinfo: a naive 1970-01-01 would
        # compare unequal here, so dropping the timezone is caught too.
        assert parsed == EPOCH
        assert parsed.tzinfo == datetime.timezone.utc
        assert (parsed.year, parsed.month, parsed.day) == (1970, 1, 1)

    def test_a_zulu_suffix_yields_a_utc_aware_instant(self):
        # Named for the observable outcome, not for the code that produces it.
        # `_parse_timestamp` rewrites a trailing "Z" into "+00:00" before
        # calling `fromisoformat`, but `fromisoformat` has accepted "Z" itself
        # since Python 3.11 and the pyamber CI matrix is 3.11/3.12/3.13 -- so
        # deleting that rewrite leaves the entire suite green, and this test
        # must not be credited with pinning it. What it does pin is the
        # resulting instant and its offset (and, uniquely in this file, the
        # "+00:00" constant, should the rewrite ever run on an older runtime).
        assert parse_timestamp("2024-05-06T07:08:09Z") == datetime.datetime(
            2024, 5, 6, 7, 8, 9, tzinfo=datetime.timezone.utc
        )

    def test_a_naive_value_is_assumed_to_be_utc(self):
        assert parse_timestamp("2024-05-06T07:08:09") == datetime.datetime(
            2024, 5, 6, 7, 8, 9, tzinfo=datetime.timezone.utc
        )

    def test_an_explicit_offset_is_preserved(self):
        parsed = parse_timestamp("2024-05-06T07:08:09+02:00")
        assert parsed.utcoffset() == datetime.timedelta(hours=2)
        assert parsed == datetime.datetime(
            2024, 5, 6, 5, 8, 9, tzinfo=datetime.timezone.utc
        )
