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
from core.models import State
from core.models.internal_queue import InternalQueue
from core.models.internal_marker import EndChannel, StartChannel
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


class _StubExecutor:
    """
    Records what `process_internal_marker` invokes on it so the test can
    assert the StartChannel / EndChannel branches of `data_processor`
    without standing up a real Operator.
    """

    def __init__(self):
        self.calls = []

    def produce_state_on_start(self, port_id):
        self.calls.append(("produce_state_on_start", port_id))
        return {"phase": "start"}

    def produce_state_on_finish(self, port_id):
        self.calls.append(("produce_state_on_finish", port_id))
        return {"phase": "finish"}

    def on_finish(self, port_id):
        self.calls.append(("on_finish", port_id))
        return iter([])


class TestProcessInternalMarker:
    @pytest.mark.timeout(2)
    def test_start_channel_invokes_produce_state_on_start(
        self, context, data_processor
    ):
        executor = _StubExecutor()
        context.executor_manager.executor = executor

        data_processor.process_internal_marker(StartChannel())

        # StartChannel routes to produce_state_on_start with the current
        # input port id (0 when no upstream is set), and the returned dict
        # is wrapped into a State on the output slot.
        assert executor.calls == [("produce_state_on_start", 0)]
        out = context.state_processing_manager.current_output_state
        assert isinstance(out, State)
        assert out["phase"] == "start"
        # `_executor_session` always switches once on exit.
        assert data_processor.switch_calls == 1

    @pytest.mark.timeout(2)
    def test_end_channel_flushes_state_then_drains_on_finish(
        self, context, data_processor
    ):
        executor = _StubExecutor()
        context.executor_manager.executor = executor

        data_processor.process_internal_marker(EndChannel())

        # EndChannel must call produce_state_on_finish first, switch
        # context to flush that state separately from the on_finish
        # tuple stream, then drain on_finish. The session itself adds
        # its own trailing switch on exit.
        assert executor.calls == [
            ("produce_state_on_finish", 0),
            ("on_finish", 0),
        ]
        # 1 switch from the explicit flush + 1 from `_executor_session`
        # exit. `_set_output_tuple` exits early on an empty iterator and
        # does not switch.
        assert data_processor.switch_calls == 2


class TestExecutorSession:
    @pytest.mark.timeout(2)
    def test_exception_inside_session_is_routed_to_exception_manager(
        self, context, data_processor
    ):
        # A raise inside the `with _executor_session()` block must be
        # swallowed and recorded on the exception_manager so that
        # `_post_switch_context_checks` can surface it after the next
        # switch. The session always switches back on exit.
        with data_processor._executor_session() as session:
            assert session is not None
            raise RuntimeError("boom-from-executor")

        assert context.exception_manager.has_exception()
        exc_info = context.exception_manager.get_exc_info()
        assert exc_info[0] is RuntimeError
        assert "boom-from-executor" in str(exc_info[1])
        # Exit always switches back to MainLoop, even on the failure path.
        assert data_processor.switch_calls == 1

    @pytest.mark.timeout(2)
    def test_clean_session_does_not_record_an_exception(self, context, data_processor):
        with data_processor._executor_session():
            pass

        assert not context.exception_manager.has_exception()
        # Even on the success path, the finally clause yields control
        # back to MainLoop exactly once.
        assert data_processor.switch_calls == 1


class TestRunInvariant:
    """
    `run()` enforces that exactly one of marker / state / tuple is queued per
    iteration. The invariant raises a RuntimeError otherwise — that branch
    is otherwise unreachable in the integration tests, so cover it directly.
    """

    @staticmethod
    def _drive_run_synchronously(context, monkeypatch) -> DataProcessor:
        # `run()` opens with a condition.wait() so MainLoop can hand off
        # control. Stub that out so the test thread can call run() directly
        # and reach the invariant check on the very first iteration without
        # any cross-thread coordination.
        cond = context.tuple_processing_manager.context_switch_condition
        monkeypatch.setattr(cond, "wait", lambda *a, **kw: None)
        return DataProcessor(context)

    @pytest.mark.timeout(2)
    def test_zero_queued_inputs_raises_invariant_error(self, context, monkeypatch):
        dp = self._drive_run_synchronously(context, monkeypatch)
        # Nothing is set on tpm/spm — has_marker + has_state + has_tuple == 0.
        with pytest.raises(RuntimeError) as excinfo:
            dp.run()
        assert "expected exactly one queued input" in str(excinfo.value)
        assert "marker=False, state=False, tuple=False" in str(excinfo.value)

    @pytest.mark.timeout(2)
    def test_two_queued_inputs_raises_invariant_error(self, context, monkeypatch):
        dp = self._drive_run_synchronously(context, monkeypatch)
        # Populate two slots — has_marker + has_tuple == 2.
        context.tuple_processing_manager.current_internal_marker = StartChannel()
        context.tuple_processing_manager.current_input_tuple = ("payload",)
        with pytest.raises(RuntimeError) as excinfo:
            dp.run()
        assert "expected exactly one queued input" in str(excinfo.value)
        assert "marker=True" in str(excinfo.value)
        assert "tuple=True" in str(excinfo.value)
