import pytest

from worker import DataTuple
from worker.models.control_payload import ControlPayload
from worker.models.identity import VirtualIdentity, LinkIdentity
from worker.models.internal_queue import InternalQueue, InputTuple, ControlElement, SenderChangeMarker, EndMarker, EndOfAllMarker
from worker.models.tuple import InputExhausted
from worker.util.stable_priority_queue import PrioritizedItem


class TestInternalQueue:
    @pytest.fixture
    def internal_queue(self):
        return InternalQueue()

    def test_internal_queue_cannot_put_non_internal_queue_element(self, internal_queue):
        with pytest.raises(TypeError):
            internal_queue.put(1)

        with pytest.raises(TypeError):
            internal_queue.put(PrioritizedItem((1, 1), InputTuple(DataTuple())))

        with pytest.raises(TypeError):
            internal_queue.put(DataTuple())

        with pytest.raises(TypeError):
            internal_queue.put(None)

        with pytest.raises(TypeError):
            internal_queue.put(InputExhausted())

    def test_internal_queue_can_put_internal_queue_element(self, internal_queue):
        internal_queue.put(InputTuple(DataTuple()))
        internal_queue.put(ControlElement(ControlPayload(), VirtualIdentity()))
        internal_queue.put(SenderChangeMarker(LinkIdentity()))
        internal_queue.put(EndMarker())
        internal_queue.put(EndOfAllMarker())

    def test_internal_queue_should_emit_control_first(self, internal_queue):
        elements = [InputTuple(DataTuple()), SenderChangeMarker(LinkIdentity()),
                    EndMarker(), EndOfAllMarker()]
        for element in elements:
            internal_queue.put(element)

        # enqueue a ControlElement:
        control = ControlElement(ControlPayload(), VirtualIdentity())
        internal_queue.put(control)

        assert internal_queue.get() == control

    def test_internal_queue_should_emit_stable_result(self, internal_queue):
        elements1 = [InputTuple(DataTuple()), SenderChangeMarker(LinkIdentity()),
                     EndMarker(), EndOfAllMarker()]
        for element in elements1:
            internal_queue.put(element)

        # enqueue a ControlElement:
        control1 = ControlElement(ControlPayload(), VirtualIdentity())
        internal_queue.put(control1)

        elements2 = [EndOfAllMarker(), InputTuple(DataTuple()), InputTuple(DataTuple()),
                     InputTuple(DataTuple()), EndMarker(), InputTuple(DataTuple()),
                     InputTuple(DataTuple()), SenderChangeMarker(LinkIdentity())]
        for element in elements2:
            internal_queue.put(element)

        # enqueue a ControlElement:
        control2 = ControlElement(ControlPayload(), VirtualIdentity())
        internal_queue.put(control2)

        assert internal_queue.get() == control1
        assert internal_queue.get() == control2

        for element in elements1 + elements2:
            assert internal_queue.get() == element
