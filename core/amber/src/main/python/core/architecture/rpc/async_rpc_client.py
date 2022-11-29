from collections import defaultdict
from concurrent.futures import Future
from typing import Dict

from loguru import logger

from core.architecture.managers.context import Context
from core.models.internal_queue import InternalQueue, ControlElement
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    ControlCommandV2,
    ControlReturnV2,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlInvocationV2,
    ControlPayloadV2,
    ReturnInvocationV2,
)


class AsyncRPCClient:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._context = context
        self._output_queue = output_queue
        self._send_sequences: Dict[ActorVirtualIdentity, int] = defaultdict(int)
        self._unfulfilled_promises: Dict[(ActorVirtualIdentity, int), Future] = dict()

    def send(
        self, to: ActorVirtualIdentity, control_command: ControlCommandV2
    ) -> Future:
        """
        Send the ControlCommand to the target actor.

        :param to: ActorVirtualIdentity, the receiver.
        :param control_command: ControlCommandV2, the command to be sent.
        """
        payload = set_one_of(
            ControlPayloadV2,
            ControlInvocationV2(self._send_sequences[to], command=control_command),
        )
        self._output_queue.put(ControlElement(tag=to, payload=payload))
        return self._create_future(to)

    def _create_future(self, to: ActorVirtualIdentity) -> Future:
        """
        Create a promise for the target actor, recording the CommandInvocations sent
        with a sequence, so that the promise can be fulfilled once the
        ReturnInvocation is received for the CommandInvocation.

        :param to: ActorVirtualIdentity, the receiver.
        """
        future = Future()
        self._unfulfilled_promises[(to, self._send_sequences[to])] = future
        self._send_sequences[to] += 1
        return future

    def receive(
        self, from_: ActorVirtualIdentity, return_invocation: ReturnInvocationV2
    ) -> None:
        """
        Receive the ReturnInvocation from the given actor.
        :param from_: ActorVirtualIdentity, the sender.
        :param return_invocation: ReturnInvocationV2, the return to be processed.
        """
        command_id = return_invocation.original_command_id
        self._fulfill_promise(from_, command_id, return_invocation.control_return)

    def _fulfill_promise(
        self,
        from_: ActorVirtualIdentity,
        command_id: int,
        control_return: ControlReturnV2,
    ) -> None:
        """
        Fulfill the promise with the CommandInvocation, referenced by the sequence id
        with this sender of ReturnInvocation.

        :param from_: ActorVirtualIdentity, the sender.
        :param command_id: int, paired with from_ to uniquely identify an unfulfilled
            future.
        :param control_return: ControlReturnV2m, to be used to fulfill the promise.
        """

        future: Future = self._unfulfilled_promises.get((from_, command_id))
        if future is not None:
            future.set_result(control_return)
            del self._unfulfilled_promises[(from_, command_id)]
        else:
            logger.warning(
                f"received unknown ControlReturn {control_return}, no corresponding"
                " ControlCommand found."
            )
