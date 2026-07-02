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
    CONTENT = "content"
    # Loop-control bookkeeping owned by the worker runtime, NOT user state -- it
    # never appears in the content JSON. In memory it rides on the StateFrame
    # envelope; it is materialized/serialized as its own column (parallel to
    # content) by to_tuple(...). from_tuple() returns the bare State; callers
    # that need these values read the corresponding columns off the tuple.
    LOOP_COUNTER = "loop_counter"
    LOOP_START_ID = "loop_start_id"
    SCHEMA = Schema(
        raw_schema={
            CONTENT: "STRING",
            LOOP_COUNTER: "LONG",
            LOOP_START_ID: "STRING",
        }
    )

    def to_json(self) -> str:
        return json.dumps(_to_json_value(self), separators=(",", ":"))

    @staticmethod
    def to_columns(
        content_json: str,
        loop_counter: int = 0,
        loop_start_id: str = "",
    ) -> dict:
        """The single column-name -> value mapping for the State wire/storage
        format. Both ``to_tuple`` (iceberg materialization) and the network
        sender build from this, so adding a column is a one-line change here
        rather than in every serializer.
        """
        return {
            State.CONTENT: content_json,
            State.LOOP_COUNTER: int(loop_counter),
            State.LOOP_START_ID: loop_start_id,
        }

    def to_tuple(
        self,
        loop_counter: int = 0,
        loop_start_id: str = "",
    ) -> Tuple:
        return Tuple(
            State.to_columns(self.to_json(), loop_counter, loop_start_id),
            schema=State.SCHEMA,
        )

    @classmethod
    def from_json(cls, payload: str) -> "State":
        return cls(_from_json_value(json.loads(payload)))

    @classmethod
    def from_tuple(cls, row: Tuple) -> "State":
        return cls.from_json(row[cls.CONTENT])


_TYPE_MARKER = "__texera_type__"
_PAYLOAD_MARKER = "payload"
_BYTES_TYPE = "bytes"


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
