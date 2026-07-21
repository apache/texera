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

from collections import deque

import pytest

from core.util.customized_queue.queue_base import QueueControl, QueueElement
from core.util.stoppable.stoppable_queue_blocking_thread import (
    StoppableQueueBlockingRunnable,
)


class FakeQueue:
    """An in-memory stand-in for IQueue.

    ``get`` pops entries in FIFO order so ``run`` can be driven to completion
    without real threads or blocking: once the stop sentinel is popped the
    loop is interrupted before the queue is ever exhausted.
    """

    def __init__(self):
        self.items: deque = deque()

    def put(self, item) -> None:
        self.items.append(item)

    def get(self):
        return self.items.popleft()

    def is_empty(self) -> bool:
        return not self.items


class RecordingRunnable(StoppableQueueBlockingRunnable):
    """Records the lifecycle callbacks so their order can be asserted."""

    def __init__(self, name: str, queue: FakeQueue):
        super().__init__(name=name, queue=queue)
        self.events: list = []

    def pre_start(self) -> None:
        self.events.append(("pre_start",))

    def receive(self, next_entry) -> None:
        self.events.append(("receive", next_entry))

    def post_stop(self) -> None:
        self.events.append(("post_stop",))


class RaisingRunnable(StoppableQueueBlockingRunnable):
    """receive() raises an unexpected error to probe the finally: clause."""

    def __init__(self, name: str, queue: FakeQueue):
        super().__init__(name=name, queue=queue)
        self.post_stop_called = 0

    def receive(self, next_entry) -> None:
        raise ValueError("boom")

    def post_stop(self) -> None:
        self.post_stop_called += 1


class TestStop:
    def test_stop_enqueues_the_runnable_stop_sentinel(self):
        queue = FakeQueue()
        runnable = StoppableQueueBlockingRunnable(name="r", queue=queue)

        runnable.stop()

        assert list(queue.items) == [StoppableQueueBlockingRunnable.RUNNABLE_STOP]
        # The sentinel is exactly the marker control, not merely truthy.
        assert queue.items[0] is StoppableQueueBlockingRunnable.RUNNABLE_STOP


class TestInterruptibleGet:
    def test_returns_a_normal_entry_unchanged(self):
        queue = FakeQueue()
        entry = QueueElement()
        queue.put(entry)
        runnable = StoppableQueueBlockingRunnable(name="r", queue=queue)

        assert runnable.interruptible_get() is entry

    def test_raises_interrupt_on_the_stop_sentinel(self):
        queue = FakeQueue()
        queue.put(StoppableQueueBlockingRunnable.RUNNABLE_STOP)
        runnable = StoppableQueueBlockingRunnable(name="r", queue=queue)

        with pytest.raises(StoppableQueueBlockingRunnable.InterruptRunnable):
            runnable.interruptible_get()

    def test_only_the_exact_sentinel_message_interrupts(self):
        # A control whose message differs from the marker is a normal entry
        # and must flow through, so unrelated controls do not stop the loop.
        queue = FakeQueue()
        other = QueueControl(msg="something-else")
        queue.put(other)
        runnable = StoppableQueueBlockingRunnable(name="r", queue=queue)

        assert runnable.interruptible_get() is other


class TestRun:
    def test_processes_entries_until_interrupted_then_posts_stop(self):
        queue = FakeQueue()
        first, second = QueueElement(), QueueElement()
        queue.put(first)
        queue.put(second)
        queue.put(StoppableQueueBlockingRunnable.RUNNABLE_STOP)
        runnable = RecordingRunnable(name="r", queue=queue)

        runnable.run()

        # pre_start first, both entries received in FIFO order, post_stop last.
        assert runnable.events == [
            ("pre_start",),
            ("receive", first),
            ("receive", second),
            ("post_stop",),
        ]

    def test_post_stop_runs_even_when_receive_raises(self):
        queue = FakeQueue()
        queue.put(QueueElement())
        runnable = RaisingRunnable(name="r", queue=queue)

        # An unexpected error is not swallowed as an interruption; it
        # propagates, but the finally: clause still tears down via post_stop.
        with pytest.raises(ValueError, match="boom"):
            runnable.run()

        assert runnable.post_stop_called == 1

    def test_stop_then_run_terminates_immediately(self):
        # Enqueuing the sentinel via stop() must be enough to end a run with
        # no entries processed in between.
        queue = FakeQueue()
        runnable = RecordingRunnable(name="r", queue=queue)
        runnable.stop()

        runnable.run()

        assert runnable.events == [("pre_start",), ("post_stop",)]
