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
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from core.architecture.rpc.async_rpc_client import AsyncRPCClient, async_run
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
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

    def test_silently_logs_when_no_matching_promise_exists(self):
        client = _make_client()
        # No prior _create_future — nothing to match. Method must not raise.
        client._fulfill_promise(
            self._channel("A"), command_id=99, control_return=ControlReturn()
        )

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
