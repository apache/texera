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

import inspect
import pytest
from collections import deque

from pytexera import *
from pytexera.udf.examples.count_batch_operator import CountBatchOperator


class TestCountBatchOperator:
    @pytest.fixture
    def count_batch_operator(self):
        return CountBatchOperator()

    @staticmethod
    def _run_batches(operator, *stages):
        operator.open()
        for count, batch_size in stages:
            if batch_size is not None:
                operator.BATCH_SIZE = batch_size
            for _ in range(count):
                deque(
                    operator.process_tuple(Tuple({"test-1": "hello", "test-2": 10}), 0)
                )
        deque(operator.on_finish(0))
        batch_counter = operator.count
        operator.close()
        return batch_counter

    @staticmethod
    def _assert_invalid_batch_size(value_expr: str, expected_msg: str):
        with pytest.raises(ValueError) as exc_info:
            operator_string = str(inspect.getsource(CountBatchOperator)).replace(
                "BATCH_SIZE = 10", f"BATCH_SIZE = {value_expr}"
            )
            operator_string += "operator = CountBatchOperator()"
            exec(operator_string)
        assert exc_info.value.args[0] == expected_msg

    def test_count_batch_operator(self, count_batch_operator):
        assert self._run_batches(count_batch_operator, (27, None)) == 3

    def test_count_batch_operator_simple(self, count_batch_operator):
        assert self._run_batches(count_batch_operator, (20, None)) == 2

    def test_count_batch_operator_medium(self, count_batch_operator):
        assert self._run_batches(count_batch_operator, (27, None)) == 3

    def test_count_batch_operator_hard(self, count_batch_operator):
        assert self._run_batches(count_batch_operator, (27, 10), (27, 5)) == 9

    def test_edge_case_string(self):
        self._assert_invalid_batch_size(
            '"test"', "BATCH_SIZE cannot be " + str(type("test")) + "."
        )

    def test_edge_case_non_positive(self, count_batch_operator):
        self._assert_invalid_batch_size("-20", "BATCH_SIZE should be positive.")

    def test_edge_case_none(self, count_batch_operator):
        self._assert_invalid_batch_size("None", "BATCH_SIZE cannot be None.")
