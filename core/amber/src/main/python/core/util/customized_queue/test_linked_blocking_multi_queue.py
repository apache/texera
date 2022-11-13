import random
import time
from threading import Thread

import pytest

from core.util.customized_queue.linked_blocking_multi_queue import (
    LinkedBlockingMultiQueue,
)


class TestLinkedBlockingMultiQueue:
    @pytest.fixture
    def queue(self):
        return LinkedBlockingMultiQueue({str: ("control", 0), int: ("data", 9)})

    @pytest.fixture
    def multi_type_queue(self):
        return LinkedBlockingMultiQueue({str: ("control", 0), (int, float): ("data",
                                                                             9)})
    def test_sub_can_emit(self, queue):
        assert queue.is_empty()
        queue.put(1)
        assert not queue.is_empty()
        assert queue.is_empty("control")
        assert queue.get() == 1
        assert queue.is_empty()
        assert queue.is_empty("control")

    def test_main_can_emit(self, queue):
        assert queue.is_empty()
        queue.put("main")
        assert not queue.is_empty()
        assert queue.get() == "main"
        assert queue.is_empty()

    def test_main_can_emit_before_sub(self, queue):
        assert queue.is_empty()
        queue.put(1)
        queue.put("s")
        assert not queue.is_empty()
        assert queue.get() == "s"
        assert queue.is_empty("control")
        assert not queue.is_empty()
        assert queue.get() == 1
        assert queue.is_empty()

    def test_can_maintain_order_respectively(self, queue):
        queue.put(1)
        queue.put("s1")
        queue.put(99)
        queue.put("s2")
        queue.put("s3")
        queue.put(3)
        queue.put("s4")
        res = list()
        while not queue.is_empty():
            res.append(queue.get())

        assert res == ["s1", "s2", "s3", "s4", 1, 99, 3]

    def test_can_disable_sub(self, queue):
        queue.disable("data")
        queue.put(1)
        queue.put("s1")
        queue.put(99)
        queue.put("s2")
        queue.put("s3")
        queue.put(3)
        queue.put("s4")
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
                queue.put(1)

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
        queue.put(1)
        consumer_thread.join()
        reraise()

    @pytest.mark.timeout(2)
    def test_producer_first_insert_main(self, queue, reraise):
        def producer():
            with reraise:
                time.sleep(0.2)
                queue.put("s")

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
        queue.put("s")
        consumer_thread.join()
        reraise()

    @pytest.mark.timeout(5)
    def test_multiple_producer_race(self, queue, reraise):
        def producer(k):
            with reraise:
                if isinstance(k, int):
                    for i in range(k):
                        queue.put(i)
                else:
                    queue.put(k)

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

    def test_multi_types(self, multi_type_queue, reraise):
        multi_type_queue.put(1)
        multi_type_queue.put(1.1)
        multi_type_queue.put("s")
        multi_type_queue.disable("data")
        assert multi_type_queue.get() == "s"
        assert multi_type_queue.is_empty()


    @pytest.mark.timeout(2)
    def test_common_single_producer_single_consumer(self, queue, reraise):
        def producer():
            with reraise:
                for i in range(11):
                    if i % 3 == 0:
                        queue.put("s")
                    else:
                        queue.put(i)

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
