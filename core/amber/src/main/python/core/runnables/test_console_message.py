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
    ControlPayloadV2,
    PythonControlMessage,
)
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity


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
    def mock_controller(self):
        return ActorVirtualIdentity("CONTROLLER")

    @pytest.mark.timeout(2)
    def test_console_message_serialization(self, mock_controller, console_message):
        """
        Test the serialization of the console message
        :param mock_controller: the mock actor id
        :param console_message: the test message
        """
        # below statements wrap the console message as the python control message
        command = set_one_of(ControlRequest, console_message)
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocation(
                method_name="ConsoleMessageTriggered", command_id=1, command=command
            ),
        )
        python_control_message = PythonControlMessage(
            tag=mock_controller, payload=payload
        )
        # serialize the python control message to bytes
        python_control_message_bytes = bytes(python_control_message)
        # deserialize the control message from bytes
        parsed_python_control_message = PythonControlMessage().parse(
            python_control_message_bytes
        )
        # deserialized one should equal to the original one
        assert python_control_message == parsed_python_control_message
