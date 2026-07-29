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

    @staticmethod
    def make_coordinator_reply() -> DCMElement:
        # The coordinator's reply to a request this worker already issued -- typically
        # worker_execution_completed, which the coordinator was still holding when it
        # terminated this worker's region, so its reply follows EndWorker.
        channel = ChannelIdentity(
            ActorVirtualIdentity(name="CONTROLLER"),
            ActorVirtualIdentity(name="worker-0"),
            is_control=True,
        )
        return DCMElement(
            tag=channel,
            payload=set_one_of(
                DirectControlMessagePayloadV2,
                ReturnInvocation(
                    command_id=4,
                    return_value=set_one_of(ControlReturn, EmptyReturn()),
                ),
            ),
        )

    @pytest.mark.timeout(2)
    def test_acknowledges_end_worker_with_only_a_queued_reply(
        self, handler, input_queue
    ):
        # A queued reply carries no work, so it must not block termination -- the
        # coordinator cannot order every reply it owes ahead of EndWorker.
        input_queue.put(self.make_coordinator_reply())

        result = asyncio.run(handler.end_worker(EmptyRequest()))

        assert isinstance(result, EmptyReturn)
        # Inspected, never consumed: the main loop still fulfills its promise.
        assert input_queue.size() == 1

    @pytest.mark.timeout(2)
    def test_rejects_end_worker_when_work_is_queued_behind_a_reply(
        self, handler, input_queue
    ):
        input_queue.put(self.make_coordinator_reply())
        input_queue.put(self.make_control_message(0))
        with pytest.raises(RuntimeError, match="unprocessed messages"):
            asyncio.run(handler.end_worker(EmptyRequest()))
        assert input_queue.size() == 2

    @pytest.mark.timeout(2)
    def test_is_idempotent_across_coordinator_retries(self, handler, input_queue):
        # The coordinator re-sends EndWorker on every termination attempt, so repeated
        # calls must neither consume nor change anything.
        input_queue.put(self.make_coordinator_reply())
        for _ in range(2):
            assert isinstance(
                asyncio.run(handler.end_worker(EmptyRequest())), EmptyReturn
            )
        assert input_queue.size() == 1
