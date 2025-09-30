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
import datetime
from core.models.internal_queue import InternalQueue
from core.util.buffer.timed_buffer import TimedBuffer
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    ControlInvocation,
    ControlRequest,
    ConsoleMessage,
    ConsoleMessageType,
)
from proto.edu.uci.ics.amber.engine.common import (
    DirectControlMessagePayloadV2,
    PythonControlMessage,
)
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity, ChannelIdentity


class TestConsoleMessage:
    @pytest.fixture
    def internal_queue(self):
        return InternalQueue()

    @pytest.fixture
    def timed_buffer(self):
        return TimedBuffer()

    @pytest.fixture
    def console_message(self):
        return ConsoleMessage(
            worker_id="0",
            timestamp=datetime.datetime.now(),
            msg_type=ConsoleMessageType.PRINT,
            source="pytest",
            title="Test Message",
            message="Test Message",
        )

    @pytest.fixture
    def mock_controller_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("CONTROLLER"), ActorVirtualIdentity("test"), True
        )

    @pytest.mark.timeout(2)
    def test_console_message_serialization(
        self, mock_controller_channel, console_message
    ):
        """
        Test the serialization of the console message
        :param mock_controller_channel: the mock control channel id
        :param console_message: the test message
        """
        # below statements wrap the console message as the python control message
        command = set_one_of(ControlRequest, console_message)
        payload = set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="ConsoleMessageTriggered", command_id=1, command=command
            ),
        )
        python_control_message = PythonControlMessage(
            tag=mock_controller_channel, payload=payload
        )
        # serialize the python control message to bytes
        python_control_message_bytes = bytes(python_control_message)
        # deserialize the control message from bytes
        parsed_python_control_message = PythonControlMessage().parse(
            python_control_message_bytes
        )
        # deserialized one should equal to the original one
        assert python_control_message == parsed_python_control_message
