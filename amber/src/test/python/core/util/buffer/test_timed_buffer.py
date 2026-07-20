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

import datetime
import types

import pytest

import core.util.buffer.timed_buffer as timed_buffer_module
from core.util.buffer.timed_buffer import TimedBuffer
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    ConsoleMessage,
    ConsoleMessageType,
)


def _make_message(title: str = "msg") -> ConsoleMessage:
    return ConsoleMessage(
        worker_id="0",
        timestamp=datetime.datetime(2024, 1, 1),
        msg_type=ConsoleMessageType.PRINT,
        source="pytest",
        title=title,
        message=title,
    )


@pytest.fixture
def clock(monkeypatch):
    """
    Replaces the ``datetime`` referenced inside ``timed_buffer`` with a
    controllable fake so that time-based flushing is fully deterministic
    (no real sleeps). ``clock.current`` is a real ``datetime`` so that
    subtraction inside the module yields a real ``timedelta``.
    """
    holder = types.SimpleNamespace(current=datetime.datetime(2024, 1, 1, 0, 0, 0))
    fake = types.SimpleNamespace(now=lambda: holder.current)
    monkeypatch.setattr(timed_buffer_module, "datetime", fake)
    return holder


def _advance(clock, seconds):
    clock.current = clock.current + datetime.timedelta(seconds=seconds)


class TestPut:
    def test_put_does_not_emit_on_its_own(self, clock):
        buffer = TimedBuffer(max_message_num=10)
        buffer.put(_make_message())
        # Below the size threshold and with no time elapsed, get() should
        # withhold the buffered message and keep it buffered.
        assert list(buffer.get()) == []
        assert len(buffer._buffer) == 1

    def test_put_preserves_order(self, clock):
        buffer = TimedBuffer(max_message_num=10)
        messages = [_make_message(f"m{i}") for i in range(3)]
        for message in messages:
            buffer.put(message)
        assert list(buffer.get(flush=True)) == messages


class TestFlushOnDemand:
    def test_flush_true_emits_all_and_clears(self, clock):
        buffer = TimedBuffer(max_message_num=10)
        messages = [_make_message(f"m{i}") for i in range(3)]
        for message in messages:
            buffer.put(message)

        emitted = list(buffer.get(flush=True))
        assert emitted == messages
        # Buffer is emptied after a flush.
        assert list(buffer.get(flush=True)) == []
        assert len(buffer._buffer) == 0

    def test_flush_true_on_empty_buffer_yields_nothing(self, clock):
        buffer = TimedBuffer(max_message_num=10)
        assert list(buffer.get(flush=True)) == []

    def test_generator_is_lazy_until_consumed(self, clock):
        # get() is a generator: side effects (clearing the buffer, resetting
        # the timer) must only happen once it is iterated.
        buffer = TimedBuffer(max_message_num=10)
        buffer.put(_make_message())
        gen = buffer.get(flush=True)
        assert len(buffer._buffer) == 1  # not yet consumed -> not cleared
        list(gen)
        assert len(buffer._buffer) == 0


class TestFlushOnSize:
    def test_reaching_max_message_num_triggers_flush(self, clock):
        buffer = TimedBuffer(max_message_num=3)
        messages = [_make_message(f"m{i}") for i in range(3)]
        for message in messages:
            buffer.put(message)
        # len(buffer) >= max_message_num -> emit everything.
        assert list(buffer.get()) == messages
        assert len(buffer._buffer) == 0

    def test_below_max_message_num_does_not_flush(self, clock):
        buffer = TimedBuffer(max_message_num=3)
        for message in [_make_message(f"m{i}") for i in range(2)]:
            buffer.put(message)
        assert list(buffer.get()) == []
        assert len(buffer._buffer) == 2

    def test_exceeding_max_message_num_triggers_flush(self, clock):
        buffer = TimedBuffer(max_message_num=2)
        messages = [_make_message(f"m{i}") for i in range(5)]
        for message in messages:
            buffer.put(message)
        assert list(buffer.get()) == messages
        assert len(buffer._buffer) == 0


class TestFlushOnTime:
    def test_elapsed_interval_triggers_flush(self, clock):
        # Interval of 2000ms -> 2.0s threshold. timedelta.seconds is an
        # integer, so 3 whole seconds (>= 2.0) triggers the time-based flush.
        buffer = TimedBuffer(max_message_num=100, max_flush_interval_in_ms=2000)
        buffer.put(_make_message())
        _advance(clock, 3)
        assert len(list(buffer.get())) == 1
        assert len(buffer._buffer) == 0

    def test_not_enough_time_elapsed_does_not_flush(self, clock):
        # 1 whole second is below the 2.0s threshold -> withhold.
        buffer = TimedBuffer(max_message_num=100, max_flush_interval_in_ms=2000)
        buffer.put(_make_message())
        _advance(clock, 1)
        assert list(buffer.get()) == []
        assert len(buffer._buffer) == 1

    def test_flush_resets_the_timer(self, clock):
        buffer = TimedBuffer(max_message_num=100, max_flush_interval_in_ms=2000)
        buffer.put(_make_message("first"))
        _advance(clock, 3)
        assert len(list(buffer.get())) == 1

        # After a flush the timer is reset, so a fresh message that is only
        # 1s old must not be flushed.
        buffer.put(_make_message("second"))
        _advance(clock, 1)
        assert list(buffer.get()) == []
        assert len(buffer._buffer) == 1

    def test_no_time_elapsed_keeps_messages_buffered(self, clock):
        buffer = TimedBuffer(max_message_num=100, max_flush_interval_in_ms=2000)
        buffer.put(_make_message())
        # Clock not advanced at all.
        assert list(buffer.get()) == []
        assert len(buffer._buffer) == 1


class TestDefaults:
    def test_default_configuration(self):
        buffer = TimedBuffer()
        assert buffer._max_message_num == 10
        assert buffer._max_flush_interval_in_ms == 500
        assert len(buffer._buffer) == 0
