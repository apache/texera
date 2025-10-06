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
from .generator_operator_binary import GeneratorOperatorBinary


class TestEchoOperator:
    @pytest.fixture
    def generator_operator_binary(self):
        return GeneratorOperatorBinary()

    def test_generator_operator_binary(self, generator_operator_binary):
        generator_operator_binary.open()
        outputs = generator_operator_binary.produce()
        output_tuple = Tuple(next(outputs))
        assert output_tuple == Tuple({"test": [1, 2, 3]})
        generator_operator_binary.close()
