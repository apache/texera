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

import sys

import pytest

from core.architecture.managers import Context
from core.models.internal_queue import InternalQueue
from core.runnables.data_processor import DataProcessor
from proto.org.apache.texera.amber.engine.architecture.rpc import ConsoleMessageType


@pytest.fixture
def context():
    return Context(worker_id="test-worker", input_queue=InternalQueue())


@pytest.fixture
def data_processor(context, monkeypatch):
    """
    DataProcessor with `_switch_context` swapped for a counter so the
    `post_switch` checks can yield without blocking the test thread and the
    test can assert exactly how many extra switches happened.
    """
    dp = DataProcessor(context)
    dp.switch_calls = 0

    def fake_switch():
        dp.switch_calls += 1

    monkeypatch.setattr(dp, "_switch_context", fake_switch)
    return dp


def _capture_exc_info() -> tuple:
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        return sys.exc_info()


class TestPostSwitchContextChecks:
    @pytest.mark.timeout(2)
    def test_no_pending_exception_is_a_no_op(self, context, data_processor):
        data_processor._post_switch_context_checks()

        assert not context.exception_manager.has_exception()
        assert (
            list(context.console_message_manager.get_messages(force_flush=True)) == []
        )
        assert data_processor.switch_calls == 0

    @pytest.mark.timeout(2)
    def test_pending_exception_is_reported_with_one_extra_switch(
        self, context, data_processor
    ):
        context.exception_manager.set_exception_info(_capture_exc_info())

        data_processor._post_switch_context_checks()

        msgs = list(context.console_message_manager.get_messages(force_flush=True))
        assert len(msgs) == 1
        msg = msgs[0]
        assert msg.worker_id == "test-worker"
        assert msg.msg_type == ConsoleMessageType.ERROR
        assert "RuntimeError: boom" in msg.title
        assert "RuntimeError: boom" in msg.message
        # Exactly one extra switch — the yield that lets MainLoop wait
        # for the resolution control message.
        assert data_processor.switch_calls == 1

    @pytest.mark.timeout(2)
    def test_pending_exception_is_cleared_after_handling(self, context, data_processor):
        context.exception_manager.set_exception_info(_capture_exc_info())

        data_processor._post_switch_context_checks()

        # Once handled, the post-switch path is idempotent: exception
        # state is cleared and the next call adds no extra console
        # message, so the worker doesn't re-pause on the same error.
        assert not context.exception_manager.has_exception()
        # Drain whatever the first call put in the buffer.
        list(context.console_message_manager.get_messages(force_flush=True))

        data_processor._post_switch_context_checks()
        msgs = list(context.console_message_manager.get_messages(force_flush=True))
        assert msgs == []
