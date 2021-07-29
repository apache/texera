from collections import defaultdict
from concurrent.futures import Future
from typing import Dict

from loguru import logger

from core.architecture.managers.context import Context
from core.models.internal_queue import ControlElement, InternalQueue
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.worker import ControlCommandV2
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlInvocationV2, ControlPayloadV2, \
    ReturnInvocationV2


class AsyncRPCClient:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._context = context
        self._output_queue = output_queue
        self._send_sequences: Dict[ActorVirtualIdentity, int] = defaultdict(int)
        self._unfulfilled_promises: Dict[(ActorVirtualIdentity, int), Future] = dict()

    def send(self, to: ActorVirtualIdentity, control_command: ControlCommandV2) -> None:
        """
        Send the ControlCommand to the target actor.

        :param to: ActorVirtualIdentity, the receiver.
        :param control_command: ControlCommandV2, the command to be sent.
        """
        payload = set_one_of(ControlPayloadV2, ControlInvocationV2(self._send_sequences[to], command=control_command))
        self._output_queue.put(ControlElement(tag=to, payload=payload))
        self.create_promise(to)

    def create_promise(self, to: ActorVirtualIdentity) -> None:
        """
        Create a promise for the target actor, recording the CommandInvocations sent with a sequence,
        so that the promise can be fulfilled once the ReturnInvocation is received for the
        CommandInvocation.

        :param to: ActorVirtualIdentity, the receiver.
        """
        # TODO: add callback api
        future = Future()
        self._unfulfilled_promises[(to, self._send_sequences[to])] = future
        logger.debug(f"future created with {(to, self._send_sequences[to])}")
        self._send_sequences[to] += 1

    def fulfill_promise(self, from_: ActorVirtualIdentity, return_invocation: ReturnInvocationV2) -> None:
        """
        Fulfill the promise with the CommandInvocation, referenced by the sequence id with this sender of
        ReturnInvocation.

        :param from_: ActorVirtualIdentity, the sender.
        :param return_invocation: ReturnInvocationV2, contains the original command id to be used to find
                        the promise, and also the ControlReturn to be used to fulfill the promise.
        """
        command_id = return_invocation.original_command_id
        future: Future = self._unfulfilled_promises[(from_, command_id)]
        future.set_result(return_invocation.control_return)
        logger.debug(f"future of {(from_, command_id)} is now fulfilled")
        del self._unfulfilled_promises[(from_, command_id)]
