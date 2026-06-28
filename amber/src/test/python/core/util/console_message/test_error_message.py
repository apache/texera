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

    def test_source_points_to_the_deepest_raising_frame(self):
        # source must encode the frame where the exception was RAISED (the
        # deepest frame), not where it was caught.
        def _inner():
            raise RuntimeError("deep failure")

        try:
            _inner()
        except RuntimeError:
            exc_info = sys.exc_info()

        msg = create_error_console_message("w0", exc_info)

        parts = msg.source.split(":")
        assert parts[0] == "test_error_message"
        assert parts[1] == "_inner"  # the raising frame, not the test method

    def test_chained_exception_reports_active_error_with_full_chain(self):
        # A `raise ... from ...` chain: the title is the active (outer) error,
        # and the message carries the whole chain including the connector.
        try:
            try:
                raise ValueError("root cause")
            except ValueError as cause:
                raise RuntimeError("wrapping error") from cause
        except RuntimeError:
            exc_info = sys.exc_info()

        msg = create_error_console_message("w0", exc_info)

        assert msg.title == "RuntimeError: wrapping error"
        assert "ValueError: root cause" in msg.message
        assert "RuntimeError: wrapping error" in msg.message
        assert "direct cause" in msg.message  # chain connector text

    def test_exception_without_message_uses_bare_class_name(self):
        try:
            raise RuntimeError
        except RuntimeError:
            exc_info = sys.exc_info()

        msg = create_error_console_message("w0", exc_info)

        assert msg.msg_type == ConsoleMessageType.ERROR
        assert msg.title == "RuntimeError"

    def test_missing_traceback_is_reported_without_crashing(self):
        # An exception object that was never raised has no traceback. The
        # reporter must still produce a message rather than throw.
        exc_info = (ValueError, ValueError("never raised"), None)

        msg = create_error_console_message("w0", exc_info)

        assert msg.msg_type == ConsoleMessageType.ERROR
        assert msg.title == "ValueError: never raised"
        assert msg.source == ""
