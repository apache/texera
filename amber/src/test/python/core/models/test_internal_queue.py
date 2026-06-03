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
from queue import Empty
from threading import Thread

import pytest

from core.models.internal_queue import InternalQueue


class TestInternalQueueGetTimeout:
    """Covers the optional `timeout` plumbed through InternalQueue.get."""

    @pytest.mark.timeout(2)
    def test_get_with_timeout_raises_empty_when_no_item(self):
        queue = InternalQueue()
        with pytest.raises(Empty):
            queue.get(timeout=0.05)

    @pytest.mark.timeout(2)
    def test_get_with_timeout_returns_available_item(self):
        queue = InternalQueue()
        queue.put("system-item")
        assert queue.get(timeout=0.05) == "system-item"

    @pytest.mark.timeout(2)
    def test_get_with_timeout_returns_item_arriving_during_wait(self, reraise):
        queue = InternalQueue()

        def producer():
            with reraise:
                time.sleep(0.1)
                queue.put("late")

        producer_thread = Thread(target=producer)
        producer_thread.start()
        assert queue.get(timeout=2) == "late"
        producer_thread.join()
        reraise()

    @pytest.mark.timeout(2)
    def test_get_without_timeout_still_blocks_until_item(self, reraise):
        # Default timeout=None must preserve the original blocking behavior.
        queue = InternalQueue()

        def producer():
            with reraise:
                time.sleep(0.1)
                queue.put("blocking")

        producer_thread = Thread(target=producer)
        producer_thread.start()
        assert queue.get() == "blocking"
        producer_thread.join()
        reraise()
