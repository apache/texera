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
import inspect
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from core.architecture.rpc import async_rpc_client as async_rpc_client_module
from core.architecture.rpc.async_rpc_client import AsyncRPCClient, async_run
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ControllerServiceStub,
    ControlReturn,
    ReturnInvocation,
)


def _make_client():
    """AsyncRPCClient with mock queue and a SimpleNamespace context.

    The constructor only reads `context.worker_id` and calls `output_queue.put`
    along the send path, so a duck-typed namespace + MagicMock queue is enough.
    """
    return AsyncRPCClient(MagicMock(), SimpleNamespace(worker_id="w0"))


class TestAsyncRunDecorator:
    def test_runs_coroutine_via_asyncio_run_when_no_loop(self):
        @async_run
        async def f():
            return 42

        # No running loop here, so the wrapper hits the RuntimeError branch
        # and dispatches via asyncio.run.
        assert f() == 42

    def test_returns_awaitable_directly_when_called_inside_running_loop(self):
        # Inside a running loop, the wrapper just calls the underlying function
        # and returns the coroutine, leaving the await to the caller.
        @async_run
        async def f():
            return "deep"

        async def driver():
            result = f()  # Must be a coroutine
            assert asyncio.iscoroutine(result)
            return await result

        assert asyncio.run(driver()) == "deep"


class TestCreateFuture:
    def test_returns_future_instance(self):
        client = _make_client()
        to = ActorVirtualIdentity(name="dest")
        fut = client._create_future(to)
        assert isinstance(fut, Future)

    def test_records_promise_at_pre_increment_sequence_and_then_increments(self):
        client = _make_client()
        to = ActorVirtualIdentity(name="dest")
        # _send_sequences starts at 0 (defaultdict(int)). _create_future stores
        # the promise at the current sequence and only THEN increments — so the
        # very first promise lives at key (to, 0).
        fut = client._create_future(to)
        assert client._unfulfilled_promises[(to, 0)] is fut
        assert client._send_sequences[to] == 1

    def test_sequence_increments_per_target_independently(self):
        client = _make_client()
        a = ActorVirtualIdentity(name="A")
        b = ActorVirtualIdentity(name="B")
        client._create_future(a)
        client._create_future(a)
        client._create_future(b)
        assert client._send_sequences[a] == 2
        assert client._send_sequences[b] == 1
        assert (a, 0) in client._unfulfilled_promises
        assert (a, 1) in client._unfulfilled_promises
        assert (b, 0) in client._unfulfilled_promises


class TestFulfillPromise:
    def _channel(self, name: str) -> ChannelIdentity:
        # `_fulfill_promise` looks up the dict by `from_.from_worker_id`; build
        # a ChannelIdentity whose sender slot matches the actor we promised to.
        return ChannelIdentity(
            from_worker_id=ActorVirtualIdentity(name=name),
            to_worker_id=ActorVirtualIdentity(name="self"),
            is_control=True,
        )

    def test_resolves_matching_future_and_clears_the_entry(self):
        client = _make_client()
        actor = ActorVirtualIdentity(name="A")
        fut = client._create_future(actor)
        ret = ControlReturn()

        client._fulfill_promise(self._channel("A"), command_id=0, control_return=ret)

        assert fut.done() and fut.result() is ret
        assert (actor, 0) not in client._unfulfilled_promises

    def test_silently_logs_when_no_matching_promise_exists(self, monkeypatch):
        client = _make_client()
        # Place an unrelated pending promise so we can verify the no-match
        # branch leaves it alone instead of silently dropping the dict entry.
        actor_b = ActorVirtualIdentity(name="B")
        fut_b = client._create_future(actor_b)
        # Patch the loguru logger used inside async_rpc_client so we can
        # assert that the no-match branch DID emit a warning. Without this
        # the implementation could silently drop unknown ControlReturns and
        # the suite would still pass.
        warning_calls = []
        monkeypatch.setattr(
            async_rpc_client_module.logger,
            "warning",
            lambda msg, *a, **kw: warning_calls.append(msg),
        )

        # No prior _create_future for actor "A" — nothing to match. Method
        # must not raise.
        client._fulfill_promise(
            self._channel("A"), command_id=99, control_return=ControlReturn()
        )

        assert len(warning_calls) == 1
        assert "no corresponding ControlCommand found" in warning_calls[0]
        # Unrelated pending promise is untouched.
        assert not fut_b.done()
        assert (actor_b, 0) in client._unfulfilled_promises

    def test_does_not_disturb_unrelated_pending_promises(self):
        client = _make_client()
        actor_a = ActorVirtualIdentity(name="A")
        actor_b = ActorVirtualIdentity(name="B")
        fut_a = client._create_future(actor_a)
        fut_b = client._create_future(actor_b)

        client._fulfill_promise(
            self._channel("A"), command_id=0, control_return=ControlReturn()
        )

        assert fut_a.done()
        assert not fut_b.done()
        assert (actor_b, 0) in client._unfulfilled_promises


class TestReceive:
    def test_delegates_command_id_and_return_value_to_fulfill_promise(self):
        client = _make_client()
        actor = ActorVirtualIdentity(name="A")
        fut = client._create_future(actor)
        ret = ControlReturn()
        invocation = ReturnInvocation(command_id=0, return_value=ret)
        from_ = ChannelIdentity(
            from_worker_id=actor,
            to_worker_id=ActorVirtualIdentity(name="self"),
            is_control=True,
        )

        client.receive(from_, invocation)

        assert fut.done() and fut.result() is ret


class TestProxyStreamBlockers:
    def test_stream_unary_blocked(self):
        client = _make_client()
        proxy = client.get_worker_interface("worker-X")
        with pytest.raises(NotImplementedError, match="_stream_unary"):
            proxy._stream_unary()

    def test_unary_stream_blocked(self):
        client = _make_client()
        proxy = client.get_worker_interface("worker-X")
        with pytest.raises(NotImplementedError, match="_unary_stream"):
            proxy._unary_stream()

    def test_stream_stream_blocked(self):
        client = _make_client()
        proxy = client.get_worker_interface("worker-X")
        with pytest.raises(NotImplementedError, match="_stream_stream"):
            proxy._stream_stream()


class TestControllerStub:
    def test_controller_stub_returns_configured_stub(self):
        client = _make_client()
        stub = client.controller_stub()
        # Identity check: same instance every call (lazily configured in __init__).
        assert stub is client._controller_service_stub
        assert stub is client.controller_stub()

    def test_controller_stub_unary_unary_is_rewired_with_async_context(self):
        # AsyncRPCClient.__init__ replaces the stub's `_unary_unary` with the
        # closure produced by `_assign_context`, then `_wrap_all_async_methods`
        # wraps that (originally async) function with `async_run`. The end
        # state is therefore: the handler is no longer the bound method from
        # ControllerServiceStub, but a synchronous async_run wrapper. A
        # regression that returned an unconfigured stub would pass the identity
        # check above, but cannot pass this one.
        client = _make_client()
        stub = client.controller_stub()
        baseline = ControllerServiceStub("")
        assert stub._unary_unary is not baseline._unary_unary
        # The _assign_context wrapper closes over the AsyncRPCClient self, so
        # if the rewiring really happened the function we end up with mentions
        # `_assign_context` somewhere in its qualname (either directly, when
        # async_run reuses the wrapped name, or via __wrapped__).
        target = getattr(stub._unary_unary, "__wrapped__", stub._unary_unary)
        assert "_assign_context" in target.__qualname__

    def test_controller_stub_async_methods_are_wrapped_with_async_run(self):
        # AsyncRPCClient also runs `_wrap_all_async_methods_with_async_run`,
        # which replaces every coroutinefunction on the stub with the sync
        # `async_run` wrapper. So whatever methods were async on a fresh
        # `ControllerServiceStub` must now be NON-coroutine on the configured
        # stub. Without this assertion the wrap-all pass could no-op silently.
        client = _make_client()
        stub = client.controller_stub()
        baseline = ControllerServiceStub("")
        async_method_names = [
            name
            for name in dir(baseline)
            if not name.startswith("_")
            and inspect.iscoroutinefunction(getattr(baseline, name))
        ]
        # Sanity: the upstream stub really does ship with async methods.
        assert async_method_names, (
            "ControllerServiceStub no longer has any async methods; this test "
            "needs to be reconsidered."
        )
        for name in async_method_names:
            assert not inspect.iscoroutinefunction(getattr(stub, name)), (
                f"{name!r} on the configured stub should have been wrapped by "
                "async_run but is still a coroutine function."
            )
