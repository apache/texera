from typing import Optional
from core.models.marker import State, Marker


class MarkerProcessingManager:
    def __init__(self):
        self.current_input_marker: Optional[Marker] = None
        self.current_output_state: Optional[State] = None

    def get_input_marker(self) -> Optional[State]:
        ret, self.current_input_marker = self.current_input_marker, None
        return ret

    def get_output_state(self) -> Optional[State]:
        ret, self.current_output_state = self.current_output_state, None
        return ret
