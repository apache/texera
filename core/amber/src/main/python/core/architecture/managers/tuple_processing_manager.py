from threading import Event, Condition
from typing import Optional, Union, Tuple, Iterator, Mapping

from core.models import InputExhausted
from proto.edu.uci.ics.amber.engine.common import LinkIdentity, LayerIdentity


class TupleProcessingManager:
    def __init__(self):
        self.current_input: Optional[Union[Tuple, InputExhausted]] = None
        self.current_input_link: Optional[LinkIdentity] = None
        self.current_input_tuple_iter: Optional[
            Iterator[Union[Tuple, InputExhausted]]
        ] = None
        self.current_output: Optional[Union[Tuple, str]] = None
        self.input_link_map: Mapping[LinkIdentity, int] = dict()
        self._context_switch_condition: Condition = Condition()
        self._context_switch_condition_with_no_pdb: Condition = Condition()
        self.finished_current: Event = Event()
        self.with_pdb = True
        self.my_id = None
        self.my_upstream_id: LayerIdentity = None

    def get_output(self) -> Optional[Union[Tuple, str]]:
        ret, self.current_output = self.current_output, None
        return ret

    @property
    def context_switch_condition(self):
        if self.with_pdb:
            return self._context_switch_condition
        else:
            return self._context_switch_condition_with_no_pdb
