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

from core.util import get_one_of, set_one_of
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ControlReturn,
    EvaluatedValue,
    TypedValue,
)


class TestSetOneOfControlReturn:
    def test_evaluated_value_survives_pack_and_unpack(self):
        # EvaluatePythonExpression is the one worker RPC whose declared reply
        # type is EvaluatedValue; it must be a registered ControlReturn oneof
        # member, otherwise set_one_of silently packs an empty ControlReturn
        # and the worker's reply is dropped on the wire.
        evaluated = EvaluatedValue(
            value=TypedValue(expression="1+1", value_str="2"),
            attributes=[],
        )

        packed = set_one_of(ControlReturn, evaluated)

        assert get_one_of(packed) == evaluated

    def test_evaluated_value_survives_wire_roundtrip(self):
        evaluated = EvaluatedValue(
            value=TypedValue(expression="1+1", value_str="2"),
            attributes=[],
        )

        wire_bytes = bytes(set_one_of(ControlReturn, evaluated))

        assert wire_bytes != b""
        assert get_one_of(ControlReturn().parse(wire_bytes)) == evaluated
