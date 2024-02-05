from threading import Event, Condition
from typing import Optional, Union, Tuple, Iterator

from core.models import InputExhausted
from proto.edu.uci.ics.amber.engine.common import PortIdentity


class TupleProcessingManager:
    def __init__(self):
        self.current_input_tuple: Optional[Union[Tuple, InputExhausted]] = None
        self.current_input_port_id: Optional[PortIdentity] = None
        self.current_input_tuple_iter: Optional[
            Iterator[Union[Tuple, InputExhausted]]
        ] = None
        self.current_output_tuple: Optional[Tuple] = None
        self.context_switch_condition: Condition = Condition()
        self.finished_current: Event = Event()

    def get_output_tuple(self) -> Optional[Tuple]:
        ret, self.current_output_tuple = self.current_output_tuple, None
        return ret
