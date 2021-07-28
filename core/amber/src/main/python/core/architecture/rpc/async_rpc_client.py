from asyncio import Future, get_running_loop
from collections import defaultdict
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

    def send(self, to: ActorVirtualIdentity, control_command: ControlCommandV2):
        logger.info(f"prepared control command {control_command}")
        self.create_promise(to, control_command)

    def create_promise(self, to: ActorVirtualIdentity, control_command: ControlCommandV2) -> None:
        payload = set_one_of(ControlPayloadV2, ControlInvocationV2(self._send_sequences[to], command=control_command))
        self._output_queue.put(ControlElement(tag=to, payload=payload))
        self._unfulfilled_promises[(to, self._send_sequences[to])] = get_running_loop().create_future()
        self._send_sequences[to] += 1

    def fulfill_promise(self, from_: ActorVirtualIdentity, return_invocation: ReturnInvocationV2) -> None:
        command_id = return_invocation.original_command_id
        future: Future = self._unfulfilled_promises[(from_, command_id)]
        future.set_result(return_invocation.control_return)
