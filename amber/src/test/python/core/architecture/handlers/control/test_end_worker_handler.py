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
from core.models.internal_queue import DataElement, DCMElement, InternalQueue
from core.models.payload import DataPayload
from core.util.proto import set_one_of
from proto.org.apache.texera.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ControlInvocation,
    ControlReturn,
    EmptyRequest,
    EmptyReturn,
    ReturnInvocation,
)
from proto.org.apache.texera.amber.engine.common import DirectControlMessagePayloadV2

CONTROL_CHANNEL = ChannelIdentity(
    ActorVirtualIdentity("CONTROLLER"),
    ActorVirtualIdentity("dummy_worker_id"),
    True,
)
DATA_CHANNEL = ChannelIdentity(
    ActorVirtualIdentity("upstream_worker_id"),
    ActorVirtualIdentity("dummy_worker_id"),
    False,
)


def _build_handler(queue: InternalQueue) -> EndWorkerHandler:
    instance = EndWorkerHandler.__new__(EndWorkerHandler)
    instance.context = SimpleNamespace(input_queue=queue)
    return instance


def _coordinator_reply_element() -> DCMElement:
    # The exact payload observed on every normal region teardown: the coordinator
    # sends EndWorker from inside its port_completed handler, so the reply to that
    # same port_completed trails EndWorker on the control channel.
    return DCMElement(
        tag=CONTROL_CHANNEL,
        payload=set_one_of(
            DirectControlMessagePayloadV2,
            ReturnInvocation(
                command_id=2,
                return_value=set_one_of(ControlReturn, EmptyReturn()),
            ),
        ),
    )


def _control_invocation_element() -> DCMElement:
    return DCMElement(
        tag=CONTROL_CHANNEL,
        payload=set_one_of(
            DirectControlMessagePayloadV2,
            ControlInvocation(method_name="QueryStatistics", command_id=1),
        ),
    )


class TestEndWorkerHandler:
    @pytest.fixture
    def queue(self):
        return InternalQueue()

    @pytest.fixture
    def handler(self, queue):
        return _build_handler(queue)

    def test_returns_empty_return_when_queue_is_empty(self, handler):
        result = asyncio.run(handler.end_worker(EmptyRequest()))
        assert isinstance(result, EmptyReturn)

    @pytest.mark.timeout(2)
    def test_succeeds_and_keeps_a_queued_coordinator_reply(self, handler, queue):
        queue.put(_coordinator_reply_element())

        result = asyncio.run(handler.end_worker(EmptyRequest()))

        assert isinstance(result, EmptyReturn)
        # The reply must still be queued afterwards: the previous implementation
        # called input_queue.get() inside its log message and silently dropped it.
        assert queue.size() == 1

    @pytest.mark.timeout(2)
    def test_fails_when_a_control_invocation_is_queued(self, handler, queue):
        queue.put(_control_invocation_element())

        with pytest.raises(RuntimeError, match="worker still has unprocessed messages"):
            asyncio.run(handler.end_worker(EmptyRequest()))
        assert queue.size() == 1

    @pytest.mark.timeout(2)
    def test_fails_when_a_control_invocation_is_queued_behind_a_reply(
        self, handler, queue
    ):
        queue.put(_coordinator_reply_element())
        queue.put(_control_invocation_element())

        with pytest.raises(RuntimeError, match="worker still has unprocessed messages"):
            asyncio.run(handler.end_worker(EmptyRequest()))
        assert queue.size() == 2

    @pytest.mark.timeout(2)
    def test_fails_when_data_is_queued(self, handler, queue):
        queue.put(DataElement(tag=DATA_CHANNEL, payload=DataPayload()))

        with pytest.raises(RuntimeError, match="worker still has unprocessed messages"):
            asyncio.run(handler.end_worker(EmptyRequest()))
        assert queue.size() == 1

    @pytest.mark.timeout(2)
    def test_is_idempotent_across_coordinator_retries(self, handler, queue):
        # The coordinator re-sends EndWorker to every worker on every termination
        # attempt, so repeated calls must neither consume nor change anything.
        queue.put(_coordinator_reply_element())

        for _ in range(2):
            result = asyncio.run(handler.end_worker(EmptyRequest()))
            assert isinstance(result, EmptyReturn)
        assert queue.size() == 1
