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

import sys

from core.util.console_message.error_message import create_error_console_message
from proto.org.apache.texera.amber.engine.architecture.rpc import ConsoleMessageType


class TestCreateErrorConsoleMessage:
    def test_builds_error_console_message_from_exc_info(self):
        # create_error_console_message turns an exc_info into a single ERROR
        # ConsoleMessage: title is the exception's final line, message is the
        # full formatted traceback, and source encodes the raising frame.
        try:
            raise ValueError("boom from udf")
        except ValueError:
            exc_info = sys.exc_info()

        msg = create_error_console_message("worker-7", exc_info)

        assert msg.worker_id == "worker-7"
        assert msg.msg_type == ConsoleMessageType.ERROR
        assert msg.title == "ValueError: boom from udf"
        assert "Traceback (most recent call last)" in msg.message
        assert "ValueError: boom from udf" in msg.message
        # source encodes "<module>:<func>:<line>" of the raising frame
        parts = msg.source.split(":")
        assert len(parts) == 3
        assert parts[0] == "test_error_message"
        assert parts[1] == "test_builds_error_console_message_from_exc_info"
