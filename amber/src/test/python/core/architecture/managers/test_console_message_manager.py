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

from datetime import datetime, timedelta

from core.architecture.managers.console_message_manager import ConsoleMessageManager
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ConsoleMessage,
    ConsoleMessageType,
)


def _msg(title: str) -> ConsoleMessage:
    return ConsoleMessage(
        worker_id="w0",
        timestamp=datetime.now(),
        msg_type=ConsoleMessageType.PRINT,
        source="src",
        title=title,
        message=title,
    )


class TestConsoleMessageManager:
    def test_initially_force_flush_drains_empty(self):
        mgr = ConsoleMessageManager()
        # No messages put yet — force_flush still yields zero items.
        assert list(mgr.get_messages(force_flush=True)) == []

    def test_force_flush_drains_all_buffered_in_order(self):
        mgr = ConsoleMessageManager()
        for t in ("a", "b", "c"):
            mgr.put_message(_msg(t))
        flushed = list(mgr.get_messages(force_flush=True))
        assert [m.title for m in flushed] == ["a", "b", "c"]
        # A second drain must come back empty — get() is consumptive.
        assert list(mgr.get_messages(force_flush=True)) == []

    def test_get_without_flush_below_threshold_yields_nothing(self):
        # Below max_message_num (default 10) and within max_flush_interval
        # (default 500ms) — the underlying TimedBuffer should withhold output.
        # Pin `_last_output_time` to "now" right before the assertion so the
        # `(now - _last_output_time).seconds >= 1` branch can't fire if the
        # rest of the test happens to run more than ~1s after construction.
        mgr = ConsoleMessageManager()
        mgr.put_message(_msg("only"))
        mgr.print_buf._last_output_time = datetime.now()
        assert list(mgr.get_messages(force_flush=False)) == []
        # The withheld message must still be drainable on a force flush.
        assert [m.title for m in mgr.get_messages(force_flush=True)] == ["only"]

    def test_get_without_flush_at_or_over_max_message_num_drains(self):
        # Once buffered count crosses max_message_num (default 10), the
        # buffer should auto-flush even without force_flush=True.
        mgr = ConsoleMessageManager()
        for i in range(10):
            mgr.put_message(_msg(f"m{i}"))
        flushed = [m.title for m in mgr.get_messages(force_flush=False)]
        assert flushed == [f"m{i}" for i in range(10)]

    def test_get_drains_when_last_output_time_is_stale(self):
        # Backdate the buffer's `_last_output_time` directly so the
        # >=500ms branch fires even with a single message and
        # force_flush=False, without sleeping or monkeypatching `datetime`.
        mgr = ConsoleMessageManager()
        mgr.put_message(_msg("stale"))
        mgr.print_buf._last_output_time = datetime.now() - timedelta(seconds=2)
        flushed = [m.title for m in mgr.get_messages(force_flush=False)]
        assert flushed == ["stale"]
