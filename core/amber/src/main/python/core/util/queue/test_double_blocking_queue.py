from threading import Thread

import pytest

from core.util.queue.double_blocking_queue import DoubleBlockingQueue


class TestDoubleBlockingQueue:

    @pytest.fixture
    def queue(self):
        return DoubleBlockingQueue(int)

    def test_sub_can_emit(self, queue):
        assert queue.empty()
        queue.put(1)
        assert not queue.empty()
        assert queue._main_empty()
        assert queue.get() == 1
        assert queue.empty()
        assert queue._main_empty()

    def test_main_can_emit(self, queue):
        assert queue.empty()
        queue.put("main")
        assert not queue.empty()
        assert queue.get() == "main"
        assert queue.empty()

    def test_main_can_emit_before_sub(self, queue):
        assert queue.empty()
        queue.put(1)
        queue.put("s")
        assert not queue.empty()
        assert queue.get() == "s"
        assert queue._main_empty()
        assert not queue.empty()
        assert queue.get() == 1
        assert queue.empty()

    def test_can_maintain_order_respectively(self, queue):
        queue.put(1)
        queue.put("s1")
        queue.put(99)
        queue.put("s2")
        queue.put("s3")
        queue.put(3)
        queue.put("s4")
        l = list()
        while not queue.empty():
            l.append(queue.get())

        assert l == ["s1", "s2", "s3", "s4", 1, 99, 3]

    def test_can_disable_sub(self, queue):
        queue.disable_sub()
        queue.put(1)
        queue.put("s1")
        queue.put(99)
        queue.put("s2")
        queue.put("s3")
        queue.put(3)
        queue.put("s4")
        l = list()
        while not queue.empty():
            l.append(queue.get())

        assert l == ["s1", "s2", "s3", "s4"]
        assert queue.empty()
        queue.enable_sub()
        assert not queue.empty()
        l = list()
        while not queue.empty():
            l.append(queue.get())

        assert l == [1, 99, 3]
        assert queue.empty()

    def test_producer_first_insert_sub(self, queue, reraise):
        def producer():
            with reraise:
                queue.put(1)

        producer_thread = Thread(target=producer)
        producer_thread.start()
        producer_thread.join()
        assert queue.get() == 1
        reraise()

    def test_consumer_first_insert_sub(self, queue, reraise):
        def consumer():
            with reraise:
                assert queue.get() == 1

        consumer_thread = Thread(target=consumer)
        consumer_thread.start()
        queue.put(1)
        consumer_thread.join()
        reraise()

    def test_producer_first_insert_main(self, queue, reraise):
        def producer():
            with reraise:
                queue.put("s")

        producer_thread = Thread(target=producer)
        producer_thread.start()
        producer_thread.join()
        assert queue.get() == "s"
        reraise()

    def test_consumer_first_insert_main(self, queue, reraise):
        def consumer():
            with reraise:
                assert queue.get() == "s"

        consumer_thread = Thread(target=consumer)
        consumer_thread.start()
        queue.put("s")
        consumer_thread.join()
        reraise()
