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

import base64
import json
from typing import Any

from .schema import Schema
from .tuple import Tuple


class State(dict):
    pass

STATE_CONTENT = "content"
_TYPE_MARKER = "__texera_type__"
_PAYLOAD_MARKER = "payload"
_BYTES_TYPE = "bytes"

STATE_SCHEMA = Schema(raw_schema={STATE_CONTENT: "STRING"})


def serialize_state(state: State) -> Tuple:
    return Tuple(
        {STATE_CONTENT: json.dumps(_to_json_value(state), separators=(",", ":"))},
        schema=STATE_SCHEMA,
    )


def deserialize_state(row: Tuple) -> State:
    return State(_from_json_value(json.loads(row[STATE_CONTENT])))


def _to_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return {
            _TYPE_MARKER: _BYTES_TYPE,
            _PAYLOAD_MARKER: base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, dict):
        return {str(key): _to_json_value(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_value(inner) for inner in value]
    raise TypeError(
        f"State value of type {type(value).__name__} is not JSON serializable"
    )


def _from_json_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_from_json_value(inner) for inner in value]
    if isinstance(value, dict):
        if value.get(_TYPE_MARKER) == _BYTES_TYPE:
            return base64.b64decode(value[_PAYLOAD_MARKER])
        return {key: _from_json_value(inner) for key, inner in value.items()}
    return value
