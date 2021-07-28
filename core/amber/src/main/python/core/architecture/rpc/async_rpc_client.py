from asyncio import base_futures, exceptions
from collections import defaultdict
from typing import Callable, Dict

from loguru import logger

from core.architecture.managers.context import Context
from core.models.internal_queue import ControlElement, InternalQueue
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.worker import ControlCommandV2
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlInvocationV2, ControlPayloadV2, \
    ReturnInvocationV2

_PENDING = base_futures._PENDING
_CANCELLED = base_futures._CANCELLED
_FINISHED = base_futures._FINISHED


class Future:
    _state = _PENDING
    _result = None
    _exception = None
    _loop = None
    _source_traceback = None
    _cancel_message = None
    # A saved CancelledError for later chaining as an exception context.
    _cancelled_exc = None

    def __init__(self):
        """Initialize the future.
        """
        self._callbacks = []

    def result(self):
        """Return the result this future represents.
        If the future has been cancelled, raises CancelledError.  If the
        future's result isn't yet available, raises InvalidStateError.  If
        the future is done and has an exception set, this exception is raised.
        """
        if self._state == _CANCELLED:
            exc = self._make_cancelled_error()
            raise exc
        if self._state != _FINISHED:
            raise exceptions.InvalidStateError('Result is not ready.')
        if self._exception is not None:
            raise self._exception
        return self._result

    def done(self):
        """Return True if the future is done.
        Done means either that a result / exception are available, or that the
        future was cancelled.
        """
        return self._state != _PENDING

    def exception(self):
        """Return the exception that was set on this future.
        The exception (or None if no exception was set) is returned only if
        the future is done.  If the future has been cancelled, raises
        CancelledError.  If the future isn't done yet, raises
        InvalidStateError.
        """
        if self._state == _CANCELLED:
            exc = self._make_cancelled_error()
            raise exc
        if self._state != _FINISHED:
            raise exceptions.InvalidStateError('Exception is not set.')
        return self._exception

    def add_done_callback(self, fn: Callable):
        """Add a callback to be run when the future becomes done.
        The callback is called with a single argument - the future object.
        """

        self._callbacks.append(fn)

    def remove_done_callback(self, fn: Callable):
        """Remove all instances of a callback from the "call when done" list.
        Returns the number of callbacks removed.
        """
        filtered_callbacks = [f
                              for f in self._callbacks
                              if f != fn]
        removed_count = len(self._callbacks) - len(filtered_callbacks)
        if removed_count:
            self._callbacks[:] = filtered_callbacks
        return removed_count

    def set_result(self, result):
        """Mark the future done and set its result.
        If the future is already done when this method is called, raises
        InvalidStateError.
        """
        if self._state != _PENDING:
            raise exceptions.InvalidStateError(f'{self._state}: {self!r}')
        self._result = result
        self._state = _FINISHED
        self._execute_callback()

    def set_exception(self, exception):
        """Mark the future done and set an exception.
        If the future is already done when this method is called, raises
        InvalidStateError.
        """
        if self._state != _PENDING:
            raise exceptions.InvalidStateError(f'{self._state}: {self!r}')
        if isinstance(exception, type):
            exception = exception()
        if type(exception) is StopIteration:
            raise TypeError("StopIteration interacts badly with generators "
                            "and cannot be raised into a Future")
        self._exception = exception
        self._state = _FINISHED
        self._execute_callback()

    def _make_cancelled_error(self):
        """Create the CancelledError to raise if the Future is cancelled.
        This should only be called once when handling a cancellation since
        it erases the saved context exception value.
        """
        if self._cancel_message is None:
            exc = exceptions.CancelledError()
        else:
            exc = exceptions.CancelledError(self._cancel_message)
        exc.__context__ = self._cancelled_exc
        # Remove the reference since we don't need this anymore.
        self._cancelled_exc = None
        return exc

    def _execute_callback(self):
        for callback in self._callbacks:
            callback()


class AsyncRPCClient:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._context = context
        self._output_queue = output_queue
        self._send_sequences: Dict[ActorVirtualIdentity, int] = defaultdict(int)
        self._unfulfilled_promises: Dict[(ActorVirtualIdentity, int), Future] = dict()

    def send(self, to: ActorVirtualIdentity, control_command: ControlCommandV2):
        self.create_promise(to, control_command)

    def create_promise(self, to: ActorVirtualIdentity, control_command: ControlCommandV2) -> None:
        payload = set_one_of(ControlPayloadV2, ControlInvocationV2(self._send_sequences[to], command=control_command))
        self._output_queue.put(ControlElement(tag=to, payload=payload))

        future = Future()
        # TODO: add callback api
        self._unfulfilled_promises[(to, self._send_sequences[to])] = future
        self._send_sequences[to] += 1

    def fulfill_promise(self, from_: ActorVirtualIdentity, return_invocation: ReturnInvocationV2) -> None:
        command_id = return_invocation.original_command_id
        future: Future = self._unfulfilled_promises[(from_, command_id)]
        future.set_result(return_invocation.control_return)
        logger.info(f"future fulfilled {return_invocation.original_command_id}")
        del self._unfulfilled_promises[(from_, command_id)]
