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
it.
"""

import asyncio
from types import SimpleNamespace

import pytest

from core.architecture.handlers.control.end_worker_handler import EndWorkerHandler
from core.models.internal_queue import DCMElement, InternalQueue
from core.util.proto import set_one_of
from proto.org.apache.texera.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ControlReturn,
    EmptyRequest,
    EmptyReturn,
    ReturnInvocation,
)
from proto.org.apache.texera.amber.engine.common import DirectControlMessagePayloadV2


class TestEndWorkerHandler:
    @pytest.fixture
    def input_queue(self):
        return InternalQueue()

    @pytest.fixture
    def handler(self, input_queue):
        return EndWorkerHandler(SimpleNamespace(input_queue=input_queue))

    @staticmethod
    def make_control_message(seq: int) -> DCMElement:
        channel = ChannelIdentity(
            ActorVirtualIdentity(name="CONTROLLER"),
            ActorVirtualIdentity(name=f"worker-{seq}"),
            is_control=True,
        )
        return DCMElement(tag=channel, payload=DirectControlMessagePayloadV2())

    @staticmethod
    def make_rpc_ack(command_id: int) -> DCMElement:
        """A ReturnInvocation ack for one of this worker's own fire-and-forget
        RPCs -- the message kind that legitimately races with EndWorker."""
        channel = ChannelIdentity(
            ActorVirtualIdentity(name="COORDINATOR"),
            ActorVirtualIdentity(name="worker-0"),
            is_control=True,
        )
        return DCMElement(
            tag=channel,
            payload=set_one_of(
                DirectControlMessagePayloadV2,
                ReturnInvocation(
                    command_id=command_id,
                    return_value=set_one_of(ControlReturn, EmptyReturn()),
                ),
            ),
        )

    def test_acknowledges_end_worker_on_empty_queue(self, handler):
        result = asyncio.run(handler.end_worker(EmptyRequest()))
        assert isinstance(result, EmptyReturn)

    def test_rejects_end_worker_with_one_unprocessed_message(
        self, handler, input_queue
    ):
        # The controller resolves endWorker's reply as a failed future and
        # retries on a fixed delay (terminateWorkersWithRetry), so a
        # straggler message must fail the call — not be dropped with a
        # successful acknowledgement.
        input_queue.put(self.make_control_message(0))
        with pytest.raises(RuntimeError, match="unprocessed messages"):
            asyncio.run(handler.end_worker(EmptyRequest()))

    @pytest.mark.timeout(2)
    def test_does_not_consume_the_unprocessed_message(self, handler, input_queue):
        straggler = self.make_control_message(0)
        input_queue.put(straggler)
        with pytest.raises(RuntimeError):
            asyncio.run(handler.end_worker(EmptyRequest()))
        # The straggler must survive the failed call so the main loop can
        # still process it before the controller's retry.
        assert input_queue.size() == 1
        assert input_queue.get() is straggler

    def test_keeps_all_messages_with_multiple_stragglers(self, handler, input_queue):
        input_queue.put(self.make_control_message(0))
        input_queue.put(self.make_control_message(1))
        with pytest.raises(RuntimeError):
            asyncio.run(handler.end_worker(EmptyRequest()))
        assert input_queue.size() == 2

    @pytest.mark.timeout(2)
    def test_succeeds_after_queue_drains(self, handler, input_queue):
        # The retry protocol end-to-end: fail while a message is pending,
        # acknowledge once the queue has drained.
        input_queue.put(self.make_control_message(0))
        with pytest.raises(RuntimeError):
            asyncio.run(handler.end_worker(EmptyRequest()))
        input_queue.get()
        result = asyncio.run(handler.end_worker(EmptyRequest()))
        assert isinstance(result, EmptyReturn)

    def test_succeeds_when_only_rpc_acks_are_queued(self, handler, input_queue):
        # An ack-only backlog carries no work (the worker never awaits its
        # fire-and-forget acks), so it must not fail the kill -- failing here
        # only makes region termination retry and loop e2e tests flaky.
        input_queue.put(self.make_rpc_ack(command_id=1))
        input_queue.put(self.make_rpc_ack(command_id=2))

        result = asyncio.run(handler.end_worker(EmptyRequest()))

        assert isinstance(result, EmptyReturn)

    def test_fails_and_preserves_real_work_mixed_with_acks(self, handler, input_queue):
        # Leniency is strictly ack-only: real work queued alongside an ack
        # must fail the call AND survive in the queue for the retry to drain.
        input_queue.put(self.make_rpc_ack(command_id=1))
        work = self.make_control_message(0)
        input_queue.put(work)

        with pytest.raises(RuntimeError, match="unprocessed messages"):
            asyncio.run(handler.end_worker(EmptyRequest()))

        remaining = []
        while not input_queue.is_empty():
            remaining.append(input_queue.get())
        assert work in remaining, (
            "the queued control message must survive a failed EndWorker "
            f"so the retry can drain it; queue held: {remaining}"
        )
