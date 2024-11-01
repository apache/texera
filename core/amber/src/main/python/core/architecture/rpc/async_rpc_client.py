import asyncio
import inspect
from collections import defaultdict
from concurrent.futures import Future
from functools import wraps
from loguru import logger
from typing import Dict, TypeVar, Callable, Any, Coroutine

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

R = TypeVar("R")


def async_run(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            # Try to get the current running loop
            if asyncio.get_running_loop():
                return func(*args, **kwargs)
        except RuntimeError:
            # If there is no running loop, use asyncio.run to start one
            return asyncio.run(func(*args, **kwargs))

    return wrapper


class AsyncRPCClient:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._context = context
        self._output_queue = output_queue
        self._send_sequences: Dict[ActorVirtualIdentity, int] = defaultdict(int)
        self._unfulfilled_promises: Dict[(ActorVirtualIdentity, int), Future] = dict()
        # TODO: is this correct?
        self._controller_service_stub = ControllerServiceStub("")
        rpc_context = AsyncRpcContext(
            ActorVirtualIdentity(self._context.worker_id),
            ActorVirtualIdentity(name="CONTROLLER"),
        )
        self._controller_service_stub._unary_unary = AsyncRPCClient._assign_context(
            self, rpc_context
        )
        # Apply async_run to all async methods of the controller service stub
        self._wrap_all_async_methods_with_async_run(self._controller_service_stub)

    def _assign_context(
        self, rpc_context: AsyncRpcContext
    ) -> Callable[..., Coroutine[Any, Any, Future]]:
        """Creates an async RPC wrapper function with a context"""

        async def wrapper(
            route: str, request, response_type, timeout, deadline, metadata
        ):
            to = rpc_context.receiver
            control_command = ControlInvocation(
                method_name=route.split("/")[-1],  # Extract the method name for RPC
                command=set_one_of(ControlRequest, request),
                context=rpc_context,
                command_id=self._send_sequences[to],
            )
            payload = set_one_of(ControlPayloadV2, control_command)
            self._output_queue.put(ControlElement(tag=to, payload=payload))
            return self._create_future(to)

        return wrapper

    def _wrap_all_async_methods_with_async_run(self, instance: Any) -> None:
        """Decorates all async methods of an instance with async_run."""
        for attr_name in dir(instance):
            attr = getattr(instance, attr_name)
            if inspect.iscoroutinefunction(attr):
                setattr(instance, attr_name, async_run(attr))

    def controller_stub(self) -> ControllerServiceStub:
        """
        Returns a proxy for interacting with the controller interface.
        """
        return self._controller_service_stub

    def get_worker_interface(self, target_worker) -> WorkerServiceStub:
        """
        Returns a proxy for interacting with a worker interface.

        :param target_worker: The identifier for the target worker.
        """
        return self._create_proxy(
            WorkerServiceStub, ActorVirtualIdentity(target_worker)
        )

    def _create_proxy(self, service_class, target_worker: ActorVirtualIdentity):
        """
        Creates a dynamic proxy for the given service class, allowing
        asynchronous RPC communication with the specified target actor.

        :param service_class: The service class to be proxied.
        :param target: The target actor's identity.
        :return: An instance of the proxy class.
        """
        rpc_client = self  # to distinguish outer and inner self

        class Proxy(service_class):

            def __init__(self, target_actor: ActorVirtualIdentity):
                self.target_actor = target_actor

            async def _unary_unary(
                self, route: str, request, response_type, *, timeout, deadline, metadata
            ):
                """
                Handles unary-unary RPC calls by creating a ControlInvocation command
                and sending it to the target actor.

                :param route: The RPC route name.
                :param request: The request message to be sent.
                :param response_type: The expected response type (unused here).
                :param timeout: The RPC call timeout (unused here).
                :param deadline: The RPC call deadline (unused here).
                :param metadata: Metadata for the RPC call (unused here).
                :return: A future representing the RPC response.
                """
                rpc_context: AsyncRpcContext = AsyncRpcContext(
                    ActorVirtualIdentity(rpc_client._context.worker_id),
                    self.target_actor,
                )
                to = rpc_context.receiver
                control_command = ControlInvocation(
                    # to align with java side, only use the method name
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

        return Proxy(target_worker)

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
