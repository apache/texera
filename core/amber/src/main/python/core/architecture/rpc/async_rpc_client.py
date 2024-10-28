from collections import defaultdict
from overrides import overrides
from concurrent.futures import Future
from typing import Dict

from loguru import logger

from core.architecture.managers.context import Context
from core.models.internal_queue import InternalQueue, ControlElement
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    AsyncRpcContext,
    ReturnInvocation,
    ControlReturn,
    ControlInvocation,
    ControllerServiceStub,
    WorkerServiceStub,
    ControlRequest,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlPayloadV2,
)


class AsyncRPCClient:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._context = context
        self._output_queue = output_queue
        self._send_sequences: Dict[ActorVirtualIdentity, int] = defaultdict(int)
        self._unfulfilled_promises: Dict[(ActorVirtualIdentity, int), Future] = dict()

    def get_controller_interface(self) -> ControllerServiceStub:
        return self.create_proxy(
            ControllerServiceStub, ActorVirtualIdentity(name="CONTROLLER")
        )

    def get_worker_interface(self, target_worker) -> WorkerServiceStub:
        return self.create_proxy(WorkerServiceStub, ActorVirtualIdentity(target_worker))

    def create_proxy(self, service_class: any, target: ActorVirtualIdentity) -> any:
        rpc_client = self

        class Proxy(service_class):

            def __init__(self, target_actor: ActorVirtualIdentity):
                self.target_actor = target_actor

            async def _unary_unary(self, route: str, request, response_type):
                rpc_context: AsyncRpcContext = AsyncRpcContext(
                    ActorVirtualIdentity(rpc_client._context.worker_id),
                    self.target_actor,
                )
                to = rpc_context.receiver
                control_command = ControlInvocation(
                    method_name=route.split("/")[-1],
                    command=set_one_of(ControlRequest, request),
                    context=rpc_context,
                    command_id=rpc_client._send_sequences[to],
                )
                payload = set_one_of(
                    ControlPayloadV2,
                    control_command,
                )
                rpc_client._output_queue.put(ControlElement(tag=to, payload=payload))
                return rpc_client._create_future(to)

            def _stream_unary(self, *args, **kwargs):
                """Block the _stream_unary method."""
                raise NotImplementedError(
                    "Rpc call invokes _stream_unary, which is not supported."
                )

            def _unary_stream(self, *args, **kwargs):
                """Block the _unary_stream method."""
                raise NotImplementedError(
                    "Rpc call invokes _unary_stream, which is not supported."
                )

            def _stream_stream(self, *args, **kwargs):
                """Block the _stream_stream method."""
                raise NotImplementedError(
                    "Rpc call invokes _stream_stream, which is not supported."
                )

        return Proxy(target)

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
        self, from_: ActorVirtualIdentity, return_invocation: ReturnInvocation
    ) -> None:
        """
        Receive the ReturnInvocation from the given actor.
        :param from_: ActorVirtualIdentity, the sender.
        :param return_invocation: ReturnInvocationV2, the return to be processed.
        """
        command_id = return_invocation.command_id
        self._fulfill_promise(from_, command_id, return_invocation.return_value)

    def _fulfill_promise(
        self,
        from_: ActorVirtualIdentity,
        command_id: int,
        control_return: ControlReturn,
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
