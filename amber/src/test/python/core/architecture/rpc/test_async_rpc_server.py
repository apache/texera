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

import asyncio

import grpclib.const
import pytest
from grpclib.const import Cardinality

from core.architecture.rpc import async_rpc_server as async_rpc_server_module
from core.architecture.rpc.async_rpc_server import AsyncRPCServer
from core.models.internal_queue import DCMElement, InternalQueue
from core.util import get_one_of, set_one_of
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    AsyncRpcContext,
    ControlError,
    ControlInvocation,
    ControlRequest,
    ControlReturn,
    EmptyRequest,
    ErrorLanguage,
    ReturnInvocation,
    StringResponse,
)


def _make_handler(func=None) -> grpclib.const.Handler:
    """Builds a real grpclib Handler namedtuple around an async callable.

    AsyncRPCServer only ever touches `.func`, but using the real namedtuple
    keeps the stub shape identical to what AsyncRPCHandlerInitializer yields.
    Defaults to a no-op async callable so the handler always carries a real
    coroutine function, even in tests that never invoke it.
    """
    if func is None:

        async def func(stream):
            pass

    return grpclib.const.Handler(
        func, Cardinality.UNARY_UNARY, ControlRequest, ControlReturn
    )


def _make_server(handlers: dict) -> AsyncRPCServer:
    """AsyncRPCServer with the given handler map and a real InternalQueue.

    Bypasses __init__ because building a real Context (and therefore a real
    AsyncRPCHandlerInitializer) drags in the whole worker; receive() only
    needs `_handlers` and `_output_queue`.
    """
    server = AsyncRPCServer.__new__(AsyncRPCServer)
    server._handlers = handlers
    server._output_queue = InternalQueue()
    return server


def _make_invocation(
    method_name: str, command_id: int, context: AsyncRpcContext | None = None
) -> ControlInvocation:
    invocation = ControlInvocation(
        method_name=method_name,
        command=set_one_of(ControlRequest, EmptyRequest()),
        command_id=command_id,
    )
    if context is not None:
        invocation.context = context
    return invocation


def _make_channel(from_name: str, to_name: str) -> ChannelIdentity:
    return ChannelIdentity(
        from_worker_id=ActorVirtualIdentity(name=from_name),
        to_worker_id=ActorVirtualIdentity(name=to_name),
        is_control=True,
    )


def _drain_single_element(queue: InternalQueue) -> DCMElement:
    """Asserts exactly one element was enqueued and returns it."""
    assert queue.size() == 1
    element = queue.get()
    assert isinstance(element, DCMElement)
    return element


def _return_invocation_of(element: DCMElement) -> ReturnInvocation:
    # DirectControlMessagePayloadV2 uses the plain "value" oneof group,
    # unlike ControlRequest/ControlReturn which use "sealed_value".
    return get_one_of(element.payload, sealed=False)


class TestHandlerMapConstruction:
    def _server_with_mapping(self, monkeypatch, mapping: dict) -> AsyncRPCServer:
        """Runs the real __init__ against a stubbed AsyncRPCHandlerInitializer.

        This exercises the key-normalization logic in __init__ without
        constructing a real worker Context.
        """

        class StubInitializer:
            def __init__(self, context):
                pass

            def __mapping__(self):
                return mapping

        monkeypatch.setattr(
            async_rpc_server_module, "AsyncRPCHandlerInitializer", StubInitializer
        )
        return AsyncRPCServer(InternalQueue(), context=None)

    @pytest.mark.timeout(2)
    def test_keys_are_lowercased_last_path_segments(self, monkeypatch):
        pause = _make_handler()
        resume = _make_handler()
        server = self._server_with_mapping(
            monkeypatch,
            {
                "/texera.WorkerService/PauseWorker": pause,
                "/texera.WorkerService/ResumeWorker": resume,
            },
        )
        assert set(server._handlers.keys()) == {"pauseworker", "resumeworker"}
        assert server.look_up("pauseworker") is pause
        assert server.look_up("resumeworker") is resume

    @pytest.mark.timeout(2)
    def test_key_without_slash_is_just_lowercased(self, monkeypatch):
        handler = _make_handler()
        server = self._server_with_mapping(monkeypatch, {"BareName": handler})
        assert server._handlers == {"barename": handler}

    @pytest.mark.timeout(2)
    def test_look_up_itself_is_case_sensitive(self, monkeypatch):
        # look_up does a plain dict access; the lowercasing of the incoming
        # method name happens in receive(), not here. Querying with the
        # original mixed-case name must therefore fail.
        server = self._server_with_mapping(
            monkeypatch, {"/texera.WorkerService/PauseWorker": _make_handler()}
        )
        with pytest.raises(KeyError):
            server.look_up("PauseWorker")


class TestWrapAsStream:
    @pytest.mark.timeout(2)
    def test_recv_message_returns_the_wrapped_request(self):
        server = _make_server({})
        request = EmptyRequest()
        stream = server._wrap_as_stream(request)
        assert asyncio.run(stream.recv_message()) is request

    @pytest.mark.timeout(2)
    def test_send_message_stores_result_which_starts_as_none(self):
        server = _make_server({})
        stream = server._wrap_as_stream(EmptyRequest())
        assert stream.result is None
        response = StringResponse(value="stored")
        asyncio.run(stream.send_message(response))
        assert stream.result is response

    @pytest.mark.timeout(2)
    def test_recv_message_never_exhausts_and_repeats_the_same_object(self):
        # A real grpclib stream ends after one message on a unary call; this
        # synthetic stream is infinite — every recv_message() call returns
        # the very same request object again.
        server = _make_server({})
        request = EmptyRequest()
        stream = server._wrap_as_stream(request)

        async def recv_twice():
            return await stream.recv_message(), await stream.recv_message()

        first, second = asyncio.run(recv_twice())
        assert first is request
        assert second is request


class TestReceiveSuccess:
    @pytest.mark.timeout(2)
    def test_reply_carries_command_id_and_round_tripped_return_value(self):
        async def handler_func(stream):
            await stream.send_message(StringResponse(value="hello"))

        server = _make_server({"echo": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Echo", 42))

        element = _drain_single_element(server._output_queue)
        return_invocation = _return_invocation_of(element)
        assert return_invocation.command_id == 42
        assert get_one_of(return_invocation.return_value) == StringResponse(
            value="hello"
        )

    @pytest.mark.timeout(2)
    def test_handler_receives_the_unwrapped_command_variant(self):
        # receive() applies get_one_of to control_invocation.command, so the
        # handler's stream yields the inner variant (EmptyRequest here), not
        # the ControlRequest wrapper.
        received = []

        async def handler_func(stream):
            received.append(await stream.recv_message())
            await stream.send_message(StringResponse(value="ok"))

        server = _make_server({"probe": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Probe", 1))

        assert received == [EmptyRequest()]

    @pytest.mark.timeout(2)
    def test_method_name_lookup_is_case_insensitive(self):
        async def handler_func(stream):
            await stream.send_message(StringResponse(value="ok"))

        server = _make_server({"pauseworker": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("PAUSEWorker", 7))

        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        assert get_one_of(return_invocation.return_value) == StringResponse(value="ok")

    @pytest.mark.timeout(2)
    def test_handler_sending_nothing_yields_empty_control_return(self):
        # If a handler never calls send_message, stream.result stays None and
        # set_one_of(ControlReturn, None) silently produces a ControlReturn
        # with NO variant set (it tries to assign a "none_type" attribute,
        # which betterproto ignores as a oneof). A reply is still sent.
        async def handler_func(stream):
            pass

        server = _make_server({"silent": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Silent", 3))

        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        assert return_invocation.command_id == 3
        assert get_one_of(return_invocation.return_value) is None

    @pytest.mark.timeout(2)
    def test_handler_sending_a_non_control_return_yields_empty_control_return(self):
        # Wrong-return-type manifestation of the same set_one_of swallowing
        # pinned by test_handler_sending_nothing_yields_empty_control_return:
        # EmptyRequest is a ControlRequest variant, NOT a ControlReturn one,
        # so set_one_of assigns an "empty_request" attribute that ControlReturn
        # has no oneof for. The reply is still sent, but its ControlReturn has
        # no variant set and serializes to nothing.
        async def handler_func(stream):
            await stream.send_message(EmptyRequest())

        server = _make_server({"wrongtype": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("WrongType", 4))

        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        assert return_invocation.command_id == 4
        assert get_one_of(return_invocation.return_value) is None
        assert bytes(return_invocation.return_value) == b""

    @pytest.mark.timeout(2)
    def test_invocation_with_unset_command_passes_none_to_the_handler(self):
        # receive() does not guard the unwrapped command: with no oneof set
        # on control_invocation.command, get_one_of yields None and None
        # flows straight into the handler's recv_message().
        received = []

        async def handler_func(stream):
            received.append(await stream.recv_message())
            await stream.send_message(StringResponse(value="still works"))

        server = _make_server({"bare": _make_handler(handler_func)})
        invocation = ControlInvocation(method_name="Bare", command_id=8)
        server.receive(_make_channel("A", "B"), invocation)

        assert received == [None]
        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        assert return_invocation.command_id == 8
        assert get_one_of(return_invocation.return_value) == StringResponse(
            value="still works"
        )

    @pytest.mark.timeout(2)
    def test_handler_sending_twice_replies_with_the_last_message(self):
        # The synthetic stream keeps only one slot: a second send_message
        # overwrites the first, so the reply carries the last write.
        async def handler_func(stream):
            await stream.send_message(StringResponse(value="first"))
            await stream.send_message(StringResponse(value="second"))

        server = _make_server({"twice": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Twice", 14))

        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        assert get_one_of(return_invocation.return_value) == StringResponse(
            value="second"
        )


class TestReceiveError:
    @pytest.mark.timeout(2)
    def test_raising_handler_replies_with_control_error(self):
        async def handler_func(stream):
            raise ValueError("boom")

        server = _make_server({"explode": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Explode", 5))

        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        assert return_invocation.command_id == 5
        error = get_one_of(return_invocation.return_value)
        assert isinstance(error, ControlError)
        assert error.error_message == "boom"
        assert error.language == ErrorLanguage.PYTHON

    @pytest.mark.timeout(2)
    def test_unknown_method_replies_with_control_error_instead_of_raising(self):
        server = _make_server({})
        server.receive(_make_channel("A", "B"), _make_invocation("NoSuchMethod", 6))

        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        error = get_one_of(return_invocation.return_value)
        assert isinstance(error, ControlError)
        # look_up raises KeyError('nosuchmethod') and str() of a KeyError
        # keeps the repr quotes around the (already lowercased) key.
        assert error.error_message == "'nosuchmethod'"
        assert error.language == ErrorLanguage.PYTHON

    @pytest.mark.timeout(2)
    def test_exception_with_empty_message_yields_empty_error_message(self):
        # str(ValueError()) is "", so the ControlError carries an empty
        # error_message — the reply itself is still well-formed.
        async def handler_func(stream):
            raise ValueError()

        server = _make_server({"blank": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Blank", 15))

        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        assert return_invocation.command_id == 15
        error = get_one_of(return_invocation.return_value)
        assert isinstance(error, ControlError)
        assert error.error_message == ""
        assert error.language == ErrorLanguage.PYTHON


class TestNoReplyNeeded:
    @pytest.mark.timeout(2)
    def test_negative_command_id_enqueues_nothing_on_success(self):
        async def handler_func(stream):
            await stream.send_message(StringResponse(value="ignored"))

        server = _make_server({"quiet": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Quiet", -1))

        assert server._output_queue.size() == 0

    @pytest.mark.timeout(2)
    def test_negative_command_id_enqueues_nothing_on_error(self):
        async def handler_func(stream):
            raise RuntimeError("swallowed")

        server = _make_server({"quiet": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Quiet", -7))

        assert server._output_queue.size() == 0

    @pytest.mark.timeout(2)
    def test_command_id_zero_is_not_negative_so_a_reply_is_sent(self):
        async def handler_func(stream):
            await stream.send_message(StringResponse(value="zero"))

        server = _make_server({"boundary": _make_handler(handler_func)})
        server.receive(_make_channel("A", "B"), _make_invocation("Boundary", 0))

        return_invocation = _return_invocation_of(
            _drain_single_element(server._output_queue)
        )
        assert return_invocation.command_id == 0

    @pytest.mark.timeout(2)
    def test_negative_command_id_with_unknown_method_is_total_silence(self):
        # The fire-and-forget + missing-method combination: the KeyError is
        # swallowed into a ControlError that is then never sent, so the only
        # trace of the failed invocation is a log line.
        server = _make_server({})
        server.receive(_make_channel("A", "B"), _make_invocation("NoSuchMethod", -3))

        assert server._output_queue.size() == 0

    @pytest.mark.timeout(2)
    def test_no_reply_needed_predicate_boundary(self):
        assert AsyncRPCServer._no_reply_needed(-1)
        assert not AsyncRPCServer._no_reply_needed(0)
        assert not AsyncRPCServer._no_reply_needed(1)


class TestReplyRouting:
    @staticmethod
    def _ok_handler():
        async def handler_func(stream):
            await stream.send_message(StringResponse(value="routed"))

        return _make_handler(handler_func)

    @pytest.mark.timeout(2)
    def test_context_sender_and_receiver_set_routes_receiver_to_sender(self):
        server = _make_server({"route": self._ok_handler()})
        context = AsyncRpcContext(
            sender=ActorVirtualIdentity(name="CONTROLLER"),
            receiver=ActorVirtualIdentity(name="worker-1"),
        )
        server.receive(
            _make_channel("data-in", "data-out"),
            _make_invocation("Route", 9, context=context),
        )

        element = _drain_single_element(server._output_queue)
        assert element.tag == ChannelIdentity(
            from_worker_id=ActorVirtualIdentity(name="worker-1"),
            to_worker_id=ActorVirtualIdentity(name="CONTROLLER"),
            is_control=True,
        )

    @pytest.mark.timeout(2)
    def test_unset_context_falls_back_to_swapping_the_incoming_channel(self):
        server = _make_server({"route": self._ok_handler()})
        server.receive(
            _make_channel("worker-A", "worker-B"), _make_invocation("Route", 10)
        )

        element = _drain_single_element(server._output_queue)
        assert element.tag == ChannelIdentity(
            from_worker_id=ActorVirtualIdentity(name="worker-B"),
            to_worker_id=ActorVirtualIdentity(name="worker-A"),
            is_control=True,
        )

    @pytest.mark.timeout(2)
    def test_partially_set_context_also_falls_back_to_swapping(self):
        # BOTH sender and receiver names must be non-empty to use the context
        # route; with only the sender set the fallback swap is used.
        server = _make_server({"route": self._ok_handler()})
        context = AsyncRpcContext(sender=ActorVirtualIdentity(name="CONTROLLER"))
        server.receive(
            _make_channel("worker-A", "worker-B"),
            _make_invocation("Route", 11, context=context),
        )

        element = _drain_single_element(server._output_queue)
        assert element.tag == ChannelIdentity(
            from_worker_id=ActorVirtualIdentity(name="worker-B"),
            to_worker_id=ActorVirtualIdentity(name="worker-A"),
            is_control=True,
        )

    @pytest.mark.timeout(2)
    def test_context_with_only_receiver_set_also_falls_back_to_swapping(self):
        # Mirror of the sender-only case: this pins the FIRST conjunct of
        # `ctx.sender.name and ctx.receiver.name`, so dropping the sender
        # check from the condition cannot go unnoticed.
        server = _make_server({"route": self._ok_handler()})
        context = AsyncRpcContext(receiver=ActorVirtualIdentity(name="worker-1"))
        server.receive(
            _make_channel("worker-A", "worker-B"),
            _make_invocation("Route", 13, context=context),
        )

        element = _drain_single_element(server._output_queue)
        assert element.tag == ChannelIdentity(
            from_worker_id=ActorVirtualIdentity(name="worker-B"),
            to_worker_id=ActorVirtualIdentity(name="worker-A"),
            is_control=True,
        )

    @pytest.mark.timeout(2)
    def test_fallback_reply_channel_is_control_even_for_data_channel_input(self):
        server = _make_server({"route": self._ok_handler()})
        from_ = ChannelIdentity(
            from_worker_id=ActorVirtualIdentity(name="worker-A"),
            to_worker_id=ActorVirtualIdentity(name="worker-B"),
            is_control=False,
        )
        server.receive(from_, _make_invocation("Route", 12))

        element = _drain_single_element(server._output_queue)
        assert element.tag.is_control is True
