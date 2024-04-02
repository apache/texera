import typing

from proto.edu.uci.ics.amber.engine.common import PortIdentity
from proto.edu.uci.ics.amber.engine.architecture.worker import WorkerStatistics


class StatisticsManager:
    def __init__(self):
        self._input_tuple_count = {}
        self._output_tuple_count = {}
        self._data_processing_time = 0
        self._control_processing_time = 0
        self._total_execution_time = 0
        self._worker_start_time = 0

    def get_statistics(self):
        return WorkerStatistics(
            self._input_tuple_count,
            self._output_tuple_count,
            self._data_processing_time,
            self._control_processing_time,
            self._total_execution_time
            - self._data_processing_time
            - self._control_processing_time,
        )

    def increase_input_tuple_count(self, port_id: PortIdentity) -> None:
        if port_id is None:
            port = 0
        else:
            port = port_id.id

        if port not in self._input_tuple_count:
            self._input_tuple_count[port] = 0
        self._input_tuple_count[port] += 1

    def increase_output_tuple_count(self, port_id: PortIdentity = None) -> None:
        # Currently, the number of output ports is fixed to 1
        if port_id is None:
            port = 0
        else:
            port = port_id.id

        if port not in self._output_tuple_count:
            self._output_tuple_count[port] = 0
        self._output_tuple_count[port] += 1

    def increase_data_processing_time(self, time) -> None:
        self._data_processing_time += time

    def increase_control_processing_time(self, time) -> None:
        self._control_processing_time += time

    def update_total_execution_time(self, time) -> None:
        self._total_execution_time = time
