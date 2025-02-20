from typing import Dict
from collections import defaultdict

from proto.edu.uci.ics.amber.core import PortIdentity
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    WorkerStatistics,
    PortTupleCountMapping,
)


class StatisticsManager:
    def __init__(self):
        self._input_tuple_count: Dict[PortIdentity, int] = dict()
        self._output_tuple_count: Dict[PortIdentity, int] = dict()
        self._data_processing_time = 0
        self._control_processing_time = 0
        self._total_execution_time = 0
        self._worker_start_time = 0

    def get_statistics(self):
        return WorkerStatistics(
            [
                PortTupleCountMapping(port_id, count)
                for port_id, count in self._input_tuple_count.items()
            ],
            [
                PortTupleCountMapping(port_id, count)
                for port_id, count in self._output_tuple_count.items()
            ],
            self._data_processing_time,
            self._control_processing_time,
            self._total_execution_time
            - self._data_processing_time
            - self._control_processing_time,
        )

    def increase_input_tuple_count(self, port_id: PortIdentity) -> None:
        port = port_id

        self._input_tuple_count = defaultdict(int, self._input_tuple_count)
        self._input_tuple_count[port] += 1

    def increase_output_tuple_count(self, port_id: PortIdentity) -> None:
        port = port_id

        self._output_tuple_count = defaultdict(int, self._output_tuple_count)
        self._output_tuple_count[port] += 1

    def increase_data_processing_time(self, time) -> None:
        self._data_processing_time += time

    def increase_control_processing_time(self, time) -> None:
        self._control_processing_time += time

    def update_total_execution_time(self, time) -> None:
        self._total_execution_time = time
