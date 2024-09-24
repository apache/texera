from threading import Event, Condition
from typing import Optional, Tuple, Iterator

from proto.edu.uci.ics.amber.engine.common import PortIdentity


class TupleProcessingManager:
    def __init__(self):
        self.current_input_tuple: Optional[Tuple] = None
        self.current_input_port_id: Optional[PortIdentity] = None
        self.current_input_tuple_iter: Optional[Iterator[Tuple]] = None
        self.current_output_tuple: Optional[Tuple] = None
        self.context_switch_condition: Condition = Condition()
        self.finished_current: Event = Event()

    def get_input_tuple(self) -> Optional[Tuple]:
        ret, self.current_input_tuple = self.current_input_tuple, None
        return ret

    def get_output_tuple(self) -> Optional[Tuple]:
        ret, self.current_output_tuple = self.current_output_tuple, None
        return ret

    def get_input_port_id(self) -> int:
        port_id = self.current_input_port_id
        # no upstream, special case for source executor.
        if port_id is None:
            return 0
        return port_id.id
