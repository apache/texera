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

import pytest
import random
import time
from threading import Thread

from core.util.customized_queue.linked_blocking_multi_queue import (
    LinkedBlockingMultiQueue,
)


@pytest.fixture
def queue():
    """A queue with `control` at priority 0 and `data` at priority 1."""
    lbmq = LinkedBlockingMultiQueue()
    lbmq.add_sub_queue("control", 0)
    lbmq.add_sub_queue("data", 1)
    return lbmq


def chain_items(sub_queue):
    """The items still reachable from a SubQueue's sentinel head, in order."""
    items = []
    node = sub_queue.head.next
    while node is not None:
        items.append(node.item)
        node = node.next
    return items


class TestLinkedBlockingMultiQueue:
    def test_sub_can_emit(self, queue):
        assert queue.is_empty()
        queue.put("data", 1)
        assert not queue.is_empty()
        assert queue.is_empty("control")
        assert queue.get() == 1
        assert queue.is_empty()
        assert queue.is_empty("control")

    def test_main_can_emit(self, queue):
        assert queue.is_empty()
        queue.put("control", "s")
        assert not queue.is_empty()
        assert queue.get() == "s"
        assert queue.is_empty()

    def test_main_can_emit_before_sub(self, queue):
        assert queue.is_empty()
        queue.put("data", 1)
        queue.put("control", "s")
        assert not queue.is_empty()
        assert queue.get() == "s"
        assert queue.is_empty("control")
        assert not queue.is_empty()
        assert queue.get() == 1
        assert queue.is_empty()

    def test_late_higher_priority_queue_is_selected_first(self):
        queue = LinkedBlockingMultiQueue()
        queue.add_sub_queue("data", 2)
        queue.add_sub_queue("control", 1)
        queue.put("data", "data message")
        queue.put("control", "control message")

        assert [group.priority for group in queue.priority_groups] == [1, 2]
        assert queue.get() == "control message"
        assert queue.get() == "data message"

    def test_can_maintain_order_respectively(self, queue):
        queue.put("data", 1)
        queue.put("control", "s1")
        queue.put("data", 99)
        queue.put("control", "s2")
        queue.put("control", "s3")
        queue.put("data", 3)
        queue.put("control", "s4")
        res = list()
        while not queue.is_empty():
            res.append(queue.get())

        assert res == ["s1", "s2", "s3", "s4", 1, 99, 3]

    def test_can_disable_sub(self, queue):
        queue.disable("data")
        queue.put("data", 1)
        queue.put("control", "s1")
        queue.put("data", 99)
        queue.put("control", "s2")
        queue.put("control", "s3")
        queue.put("data", 3)
        queue.put("control", "s4")
        res = list()
        while not queue.is_empty():
            res.append(queue.get())

        assert res == ["s1", "s2", "s3", "s4"]
        assert queue.is_empty()
        queue.enable("data")
        assert not queue.is_empty()
        res = list()
        while not queue.is_empty():
            res.append(queue.get())

        assert res == [1, 99, 3]
        assert queue.is_empty()

    @pytest.mark.timeout(2)
    def test_producer_first_insert_sub(self, queue, reraise):
        def producer():
            with reraise:
                time.sleep(0.2)
                queue.put("data", 1)

        producer_thread = Thread(target=producer)
        producer_thread.start()
        producer_thread.join()
        assert queue.get() == 1
        reraise()

    @pytest.mark.timeout(2)
    def test_consumer_first_insert_sub(self, queue, reraise):
        def consumer():
            with reraise:
                assert queue.get() == 1
                assert queue.is_empty()

        consumer_thread = Thread(target=consumer)
        consumer_thread.start()
        time.sleep(0.2)
        queue.put("data", 1)
        consumer_thread.join()
        reraise()

    @pytest.mark.timeout(2)
    def test_producer_first_insert_main(self, queue, reraise):
        def producer():
            with reraise:
                time.sleep(0.2)
                queue.put("control", "s")

        producer_thread = Thread(target=producer)
        producer_thread.start()
        producer_thread.join()
        assert queue.get() == "s"
        reraise()

    @pytest.mark.timeout(2)
    def test_consumer_first_insert_main(self, queue, reraise):
        def consumer():
            with reraise:
                assert queue.get() == "s"
                assert queue.is_empty()

        consumer_thread = Thread(target=consumer)
        consumer_thread.start()
        time.sleep(0.2)
        queue.put("control", "s")
        consumer_thread.join()
        reraise()

    @pytest.mark.timeout(10)
    def test_multiple_producer_race(self, queue, reraise):
        def producer(k):
            with reraise:
                if isinstance(k, int):
                    for i in range(k):
                        queue.put("data", i)
                else:
                    queue.put("control", k)

        threads = []
        target = set()
        for i in range(1000):
            if random.random() > 0.5:
                i = chr(i)
                target.add(i)
            producer_thread = Thread(target=producer, args=(i,))
            producer_thread.start()
            threads.append(producer_thread)
        res = set()

        def consumer():
            with reraise:
                queue.disable("data")
                while len(res) < len(target):
                    res.add(queue.get())

        consumer_thread = Thread(target=consumer)
        consumer_thread.start()
        for thread in threads:
            thread.join()

        consumer_thread.join()
        assert res == target

        reraise()

    def test_multi_types(
        self,
        queue,
    ):
        queue.put("data", 1)
        queue.put("data", 1.1)
        queue.put("control", "s")
        queue.disable("data")
        assert queue.get() == "s"
        assert queue.is_empty()

    @pytest.mark.timeout(2)
    def test_common_single_producer_single_consumer(self, queue, reraise):
        def producer():
            with reraise:
                for i in range(11):
                    if i % 3 == 0:
                        queue.put("control", "s")
                    else:
                        queue.put("data", i)

        producer_thread = Thread(target=producer)
        producer_thread.start()
        producer_thread.join()

        total: int = 0
        while True:
            queue.enable("data")
            queue.enable("data")
            t = queue.get()
            queue.is_empty("control")

            if isinstance(t, int):
                total += t
            else:
                assert t == "s"
            queue.is_empty()
            queue.disable("data")
            queue.disable("data")
            queue.is_empty()
            queue.disable("data")
            if t == 10:
                break
        assert total == sum(filter(lambda x: x % 3 != 0, range(11)))

        reraise()


class TestPeek:
    """peek() at all three levels. It never waits — LinkedBlockingMultiQueue.peek
    short-circuits on total_count == 0 — so these are safe single-threaded."""

    def test_returns_none_on_empty_queue(self, queue):
        assert queue.peek() is None

    def test_does_not_consume_the_item(self, queue):
        queue.put("data", 1)
        assert queue.peek() == 1
        assert queue.peek() == 1
        assert queue.size() == 1
        assert queue.get() == 1

    def test_prefers_the_higher_priority_sub_queue(self, queue):
        queue.put("data", 1)
        queue.put("control", "s")
        assert queue.peek() == "s"

    def test_returns_none_when_the_only_non_empty_sub_queue_is_disabled(self, queue):
        queue.put("data", 1)
        queue.disable("data")
        assert queue.peek() is None

    def test_priority_group_peek_perturbs_round_robin_state(self):
        lbmq = LinkedBlockingMultiQueue()
        lbmq.add_sub_queue("first", 1)
        lbmq.add_sub_queue("second", 1)
        group = lbmq.get_sub_queue("first").priority_group
        lbmq.put("second", "s")

        assert group.next_idx == 0
        assert group.peek() == "s"
        # Skipping the empty `first` queue advanced next_idx, so peek is not
        # side-effect free on the group's round-robin cursor.
        assert group.next_idx == 1

    def test_priority_group_peek_returns_none_when_all_queues_empty(self, queue):
        group = queue.get_sub_queue("control").priority_group
        assert group.peek() is None

    def test_selection_peek_returns_first_non_none_across_groups(self, queue):
        queue.put("data", 1)
        assert queue.sub_queue_selection.peek() == 1


class TestSubQueueClear:
    def test_clear_resets_count_and_collapses_the_chain(self, queue):
        queue.put("data", 1)
        queue.put("data", 2)
        sub = queue.get_sub_queue("data")

        sub.clear()

        assert sub.size() == 0
        assert sub.is_empty()
        assert sub.head is sub.last
        assert chain_items(sub) == []
        assert queue.size() == 0

    def test_clear_leaves_total_count_alone_when_disabled(self, queue):
        queue.put("data", 1)
        queue.put("control", "s")
        queue.disable("data")
        assert queue.size() == 1

        queue.get_sub_queue("data").clear()

        assert queue.size() == 1

    def test_clear_does_not_reset_in_mem_size(self, queue):
        queue.put("data", 1)
        before = queue.in_mem_size("data")
        assert before > 0

        queue.get_sub_queue("data").clear()

        # Current behavior: clear() drops the items but not the in-memory
        # size accounting.
        assert queue.in_mem_size("data") == before


class TestSubQueueRemove:
    def test_remove_none_returns_false(self, queue):
        assert queue.get_sub_queue("data").remove(None) is False

    def test_remove_missing_item_returns_false(self, queue):
        queue.put("data", "a")
        sub = queue.get_sub_queue("data")

        assert sub.remove("missing") is False
        assert chain_items(sub) == ["a"]
        assert sub.size() == queue.size() == 1

    def test_remove_first_item(self, queue):
        for item in ["a", "b", "c"]:
            queue.put("data", item)
        sub = queue.get_sub_queue("data")

        assert sub.remove("a") is True
        assert chain_items(sub) == ["b", "c"]
        assert sub.size() == queue.size() == 2
        assert queue.get() == "b"

    def test_remove_middle_item(self, queue):
        for item in ["a", "b", "c"]:
            queue.put("data", item)
        sub = queue.get_sub_queue("data")

        assert sub.remove("b") is True
        assert chain_items(sub) == ["a", "c"]
        assert sub.size() == queue.size() == 2

    def test_remove_last_item_fixes_up_the_tail(self, queue):
        for item in ["a", "b", "c"]:
            queue.put("data", item)
        sub = queue.get_sub_queue("data")

        assert sub.remove("c") is True
        assert chain_items(sub) == ["a", "b"]
        assert sub.last.item == "b"
        # The tail pointer must still be usable for the next enqueue.
        queue.put("data", "d")
        assert chain_items(sub) == ["a", "b", "d"]

    def test_remove_only_item_collapses_the_chain(self, queue):
        queue.put("data", "only")
        sub = queue.get_sub_queue("data")

        assert sub.remove("only") is True
        assert sub.head is sub.last
        assert sub.is_empty()
        assert queue.size() == 0

    def test_remove_on_disabled_sub_queue_leaves_total_count(self, queue):
        queue.put("data", "a")
        queue.put("data", "b")
        queue.disable("data")
        assert queue.size() == 0

        assert queue.get_sub_queue("data").remove("a") is True

        # total_count already excludes a disabled sub-queue, so only the
        # sub-queue's own count moves.
        assert queue.size() == 0
        assert queue.size("data") == 1
        queue.enable("data")
        assert queue.size() == 1


class TestPriorityGroupRemoveQueue:
    def test_remove_queue_shrinks_the_group(self, queue):
        sub = queue.get_sub_queue("data")
        group = sub.priority_group

        group.remove_queue(sub)

        assert group.queues == []

    def test_remove_queue_resets_next_idx_when_it_reached_the_end(self):
        lbmq = LinkedBlockingMultiQueue()
        lbmq.add_sub_queue("first", 1)
        lbmq.add_sub_queue("second", 1)
        group = lbmq.get_sub_queue("first").priority_group
        lbmq.put("first", "a")
        assert lbmq.get() == "a"
        assert group.next_idx == 1

        group.remove_queue(lbmq.get_sub_queue("second"))

        assert len(group.queues) == 1
        assert group.next_idx == 0

    def test_remove_queue_ignores_a_non_matching_key(self, queue):
        control_group = queue.get_sub_queue("control").priority_group

        # `data` lives in a different group, so this is a no-op.
        control_group.remove_queue(queue.get_sub_queue("data"))

        assert [q.key for q in control_group.queues] == ["control"]


class TestRemoveSubQueue:
    def test_missing_key_returns_none_and_mutates_nothing(self, queue):
        assert queue.remove_sub_queue("nope") is None
        assert sorted(queue.sub_queues) == ["control", "data"]
        assert len(queue.priority_groups) == 2

    def test_removes_an_enabled_sub_queue_and_its_items(self, queue):
        queue.put("control", "s")
        queue.put("data", 1)

        removed = queue.remove_sub_queue("control")

        assert removed is not None
        assert removed.key == "control"
        assert sorted(queue.sub_queues) == ["data"]
        # The removed sub-queue's item no longer counts toward the total.
        assert queue.size() == 1

    def test_drops_the_priority_group_once_it_is_empty(self, queue):
        queue.remove_sub_queue("control")

        assert len(queue.priority_groups) == 1
        assert queue.priority_groups[0].priority == 1


class TestSmallGuards:
    def test_is_empty_tracks_size(self, queue):
        sub = queue.get_sub_queue("data")
        assert sub.is_empty()
        queue.put("data", 1)
        assert not sub.is_empty()

    def test_str_renders_an_arrow_chain(self, queue):
        sub = queue.get_sub_queue("control")
        assert str(sub) == ""
        for item in ["a", "b", "c"]:
            queue.put("control", item)
        assert str(sub) == "a -> b -> c -> "

    def test_str_only_supports_str_items(self, queue):
        queue.put("data", 1)
        with pytest.raises(TypeError):
            str(queue.get_sub_queue("data"))

    def test_put_none_raises_value_error(self, queue):
        with pytest.raises(ValueError, match="Does not support NoneType."):
            queue.put("data", None)

    def test_get_next_returns_none_when_everything_is_empty(self, queue):
        # Called directly: q.get() on an empty queue would block forever.
        assert queue.sub_queue_selection.get_next() is None

    def test_set_priority_groups_to_empty(self, queue):
        queue.put("data", 1)
        queue.sub_queue_selection.set_priority_groups([])

        assert queue.sub_queue_selection.priority_groups == []
        assert queue.sub_queue_selection.get_next() is None
        assert queue.sub_queue_selection.peek() is None

    def test_len_matches_size(self, queue):
        assert len(queue) == 0
        queue.put("data", 1)
        queue.put("control", "s")
        assert len(queue) == queue.size() == 2
