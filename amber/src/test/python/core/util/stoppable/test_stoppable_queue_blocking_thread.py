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

import time
from queue import Queue
from threading import Thread

import pytest

from core.models.internal_queue import InternalQueue
from core.util.stoppable.stoppable_queue_blocking_thread import (
    StoppableQueueBlockingRunnable,
)

RUNNABLE_STOP = StoppableQueueBlockingRunnable.RUNNABLE_STOP


class _Recorder(StoppableQueueBlockingRunnable):
    """Minimal concrete runnable that records what it receives."""

    def __init__(self, queue):
        super().__init__(name="recorder", queue=queue)
        self.received = []
        self.pre_start_calls = 0
        self.post_stop_calls = 0

    def receive(self, next_entry):
        self.received.append(next_entry)

    def pre_start(self):
        self.pre_start_calls += 1

    def post_stop(self):
        self.post_stop_calls += 1


def _wait_until(predicate, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


class TestStoppableQueueBlockingRunnable:
    @pytest.fixture(autouse=True)
    def fast_poll(self, monkeypatch):
        # keep the stop-flag recheck snappy so tests don't wait a full second
        monkeypatch.setattr(StoppableQueueBlockingRunnable, "STOP_POLL_INTERVAL", 0.05)

    @pytest.mark.timeout(5)
    def test_delivers_items_then_stops_via_marker(self):
        queue = Queue()
        runnable = _Recorder(queue)
        thread = Thread(target=runnable.run, daemon=True)
        thread.start()

        queue.put("a")
        queue.put("b")
        assert _wait_until(lambda: runnable.received == ["a", "b"])

        runnable.stop()
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert runnable.received == ["a", "b"]

    @pytest.mark.timeout(5)
    def test_stop_flag_alone_terminates_run_without_marker(self):
        # Setting only the flag (no RUNNABLE_STOP marker enqueued) must still end
        # run(): the timeout recheck is what guards against an indefinite hang.
        queue = Queue()
        runnable = _Recorder(queue)
        thread = Thread(target=runnable.run, daemon=True)
        thread.start()

        runnable._stopped.set()
        thread.join(timeout=3)
        assert not thread.is_alive()

    # ---- interruptible_get() unit-level behavior ----

    def test_interruptible_get_returns_a_normal_entry(self):
        queue = Queue()
        runnable = _Recorder(queue)
        queue.put("x")
        assert runnable.interruptible_get() == "x"

    def test_interruptible_get_raises_on_stop_marker(self):
        # A consumed RUNNABLE_STOP marker breaks the loop even when the stop
        # flag was never set (e.g. an out-of-band marker enqueued directly).
        queue = Queue()
        runnable = _Recorder(queue)
        queue.put(RUNNABLE_STOP)
        with pytest.raises(StoppableQueueBlockingRunnable.InterruptRunnable):
            runnable.interruptible_get()

    def test_interruptible_get_raises_immediately_when_stop_flag_set(self):
        # When the flag is already set the loop body never runs, so the queue is
        # never consulted and the pending item is left untouched.
        queue = Queue()
        runnable = _Recorder(queue)
        queue.put("untouched")
        runnable._stopped.set()
        with pytest.raises(StoppableQueueBlockingRunnable.InterruptRunnable):
            runnable.interruptible_get()
        assert queue.get_nowait() == "untouched"

    @pytest.mark.timeout(5)
    def test_interruptible_get_retries_after_empty_timeout(self, reraise):
        # An item arriving after at least one Empty timeout is still returned,
        # exercising the `except Empty: continue` recheck branch.
        queue = Queue()
        runnable = _Recorder(queue)

        def producer():
            with reraise:
                time.sleep(StoppableQueueBlockingRunnable.STOP_POLL_INTERVAL * 2)
                queue.put("late")

        producer_thread = Thread(target=producer)
        producer_thread.start()
        assert runnable.interruptible_get() == "late"
        producer_thread.join()
        reraise()

    # ---- stop() and lifecycle hooks ----

    def test_stop_sets_flag_and_enqueues_marker(self):
        queue = Queue()
        runnable = _Recorder(queue)
        runnable.stop()
        assert runnable._stopped.is_set()
        assert queue.get_nowait() == RUNNABLE_STOP

    @pytest.mark.timeout(5)
    def test_lifecycle_hooks_called_once_around_run(self):
        queue = Queue()
        runnable = _Recorder(queue)
        thread = Thread(target=runnable.run, daemon=True)
        thread.start()

        runnable.stop()
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert runnable.pre_start_calls == 1
        assert runnable.post_stop_calls == 1

    @pytest.mark.timeout(5)
    def test_stop_unblocks_thread_waiting_on_empty_internal_queue(self):
        # End-to-end against the real InternalQueue -> LinkedBlockingMultiQueue
        # plumbing: a thread parked on an empty queue is released by stop().
        queue = InternalQueue()
        runnable = _Recorder(queue)
        thread = Thread(target=runnable.run, daemon=True)
        thread.start()

        queue.put("hello")
        assert _wait_until(lambda: runnable.received == ["hello"])

        runnable.stop()
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert runnable.post_stop_calls == 1
