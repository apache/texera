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

from typing import Iterator

from core.util.buffer.timed_buffer import TimedBuffer
from proto.edu.uci.ics.amber.engine.architecture.rpc import ConsoleMessage


class ConsoleMessageManager:
    def __init__(self):
        self.print_buf = TimedBuffer()

    def get_messages(self, force_flush: bool = False) -> Iterator[ConsoleMessage]:
        return self.print_buf.get(force_flush)

    def put_message(self, msg: ConsoleMessage) -> None:
        self.print_buf.put(msg)
