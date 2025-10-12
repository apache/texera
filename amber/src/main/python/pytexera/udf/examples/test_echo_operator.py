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
from .echo_operator import EchoOperator


class TestEchoOperator:
    @pytest.fixture
    def echo_operator(self):
        return EchoOperator()

    def test_echo_operator(self, echo_operator):
        echo_operator.open()
        tuple_ = Tuple({"test-1": "hello", "test-2": 10})

        outputs = echo_operator.process_tuple(tuple_, 0)
        output_tuple = next(outputs)

        assert output_tuple == tuple_
        with pytest.raises(StopIteration):
            next(outputs)
