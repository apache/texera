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

from core.util.stoppable.stoppable_queue_blocking_thread import (
    StoppableQueueBlockingRunnable,
)


class _Recorder(StoppableQueueBlockingRunnable):
    """Minimal concrete runnable that records what it receives."""

    def __init__(self, queue):
        super().__init__(name="recorder", queue=queue)
        self.received = []

    def receive(self, next_entry):
        self.received.append(next_entry)


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
