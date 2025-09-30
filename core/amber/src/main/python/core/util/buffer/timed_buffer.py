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

from datetime import datetime
from typing import List, Iterator

from core.util.buffer.buffer_base import IBuffer
from proto.edu.uci.ics.amber.engine.architecture.rpc import ConsoleMessage


class TimedBuffer(IBuffer):
    def __init__(self, max_message_num=10, max_flush_interval_in_ms=500):
        self._max_message_num = max_message_num
        self._max_flush_interval_in_ms = max_flush_interval_in_ms
        self._buffer: List[ConsoleMessage]() = list()
        self._last_output_time = datetime.now()

    def put(self, message: ConsoleMessage) -> None:
        self._buffer.append(message)

    def get(self, flush: bool = False) -> Iterator[ConsoleMessage]:
        if (
            flush
            or len(self._buffer) >= self._max_message_num
            or (datetime.now() - self._last_output_time).seconds
            >= self._max_flush_interval_in_ms / 1000
        ):
            self._last_output_time = datetime.now()
            yield from self._buffer
            self._buffer.clear()
