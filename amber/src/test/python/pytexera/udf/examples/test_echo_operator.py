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
from pytexera.udf.examples.echo_operator import EchoOperator


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

    def test_on_finish_emits_a_single_none(self, echo_operator):
        # The echo operator has nothing buffered, so end-of-port emits one
        # placeholder and nothing more.
        #
        # Resolve the override out of EchoOperator.__dict__ rather than off
        # the instance: UDFOperatorV2.on_finish has a byte-identical body, so
        # a plain attribute lookup falls back to the base class and the
        # assertion below would still hold with this operator's own override
        # deleted -- i.e. it would cover line 28 without pinning it.
        on_finish = EchoOperator.__dict__["on_finish"]

        assert list(on_finish(echo_operator, 0)) == [None]
