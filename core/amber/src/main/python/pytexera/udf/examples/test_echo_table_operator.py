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

from collections import deque

import pytest

from core.models.table import all_output_to_tuple
from pytexera import Tuple
from .echo_table_operator import EchoTableOperator


class TestEchoTableOperator:
    @pytest.fixture
    def echo_table_operator(self):
        return EchoTableOperator()

    def test_echo_table_operator(self, echo_table_operator):
        echo_table_operator.open()
        tuple_ = Tuple({"test-1": "hello", "test-2": 10})
        print(tuple_)
        deque(echo_table_operator.process_tuple(tuple_, 0))
        outputs = echo_table_operator.on_finish(0)
        output_tuple = next(all_output_to_tuple(next(outputs)))
        assert output_tuple == tuple_
        with pytest.raises(StopIteration):
            next(outputs)
        echo_table_operator.close()
