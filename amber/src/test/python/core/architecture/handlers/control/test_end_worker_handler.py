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

"""EndWorker is the coordinator's acknowledgement point before it stops the
worker actor. A successful reply promises no queued work is dropped.

RPC acks (ReturnInvocations for this worker's own fire-and-forget calls, e.g.
worker_execution_completed) race with EndWorker by design: the coordinator
decides to end the worker while its acks are still in flight. An ack-only
backlog carries no work and must not fail the kill; anything else must fail
loudly AND stay in the queue so the coordinator's retry lets the worker drain
it (the old handler consumed one message as a side effect of logging it).
"""

import asyncio
from types import SimpleNamespace

import pytest

from core.architecture.handlers.control.end_worker_handler import EndWorkerHandler
from core.models.internal_queue import DCMElement, InternalQueue
from core.util.proto import set_one_of
from proto.org.apache.texera.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    AsyncRpcContext,
    ControlInvocation,
    ControlRequest,
    ControlReturn,
    EmptyRequest,
    EmptyReturn,
    ReturnInvocation,
)
from proto.org.apache.texera.amber.engine.common import DirectControlMessagePayloadV2


_CONTROL_CHANNEL = ChannelIdentity(
    from_worker_id=ActorVirtualIdentity(name="COORDINATOR"),
    to_worker_id=ActorVirtualIdentity(name="Worker:WF1-test-op-main-0"),
    is_control=True,
)


def _ack_element(command_id: int) -> DCMElement:
    return DCMElement(
        tag=_CONTROL_CHANNEL,
        payload=set_one_of(
            DirectControlMessagePayloadV2,
            ReturnInvocation(
                command_id=command_id,
                return_value=set_one_of(ControlReturn, EmptyReturn()),
            ),
        ),
    )


def _control_invocation_element(command_id: int) -> DCMElement:
    return DCMElement(
        tag=_CONTROL_CHANNEL,
        payload=set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(
                method_name="PauseWorker",
                command=set_one_of(ControlRequest, EmptyRequest()),
                context=AsyncRpcContext(),
                command_id=command_id,
            ),
        ),
    )


def _make_handler(input_queue: InternalQueue) -> EndWorkerHandler:
    return EndWorkerHandler(SimpleNamespace(input_queue=input_queue))


class TestEndWorkerHandler:
    def test_succeeds_on_empty_queue(self):
        handler = _make_handler(InternalQueue())
        result = asyncio.run(handler.end_worker(EmptyRequest()))
        assert isinstance(result, EmptyReturn)

    def test_succeeds_when_only_rpc_acks_are_queued(self):
        # Two queued acks (more than the old handler's accidental
        # consume-one-in-the-log leniency could absorb) must not fail the kill.
        queue = InternalQueue()
        queue.put(_ack_element(command_id=1))
        queue.put(_ack_element(command_id=2))
        handler = _make_handler(queue)

        result = asyncio.run(handler.end_worker(EmptyRequest()))

        assert isinstance(result, EmptyReturn)

    def test_fails_and_preserves_real_work(self):
        # A queued control invocation is real work: the handler must fail so
        # the coordinator retries, and the message must STILL be in the queue
        # afterwards (the old handler swallowed one message while logging it).
        queue = InternalQueue()
        queue.put(_ack_element(command_id=1))
        work = _control_invocation_element(command_id=2)
        queue.put(work)
        handler = _make_handler(queue)

        with pytest.raises(Exception, match="unprocessed messages"):
            asyncio.run(handler.end_worker(EmptyRequest()))

        remaining = []
        while not queue.is_empty():
            remaining.append(queue.get())
        assert work in remaining, (
            "the queued control invocation must survive a failed EndWorker "
            f"so the retry can drain it; queue held: {remaining}"
        )
