import typing


class StatisticsManager:
    def __init__(self):
        self._input_tuple_count = 0
        self._output_tuple_count = 0
        self._data_processing_time = 0
        self._control_processing_time = 0
        self._total_execution_time = 0
        self._worker_start_time = 0

    def get_statistics(self) -> typing.Tuple[int, int]:
        return (
            self._input_tuple_count,
            self._output_tuple_count,
            self._data_processing_time,
            self._control_processing_time,
            self._total_execution_time
            - self._data_processing_time
            - self._control_processing_time,
        )

    def increase_input_tuple_count(self) -> None:
        self._input_tuple_count += 1

    def increase_output_tuple_count(self) -> None:
        self._output_tuple_count += 1

    def increase_data_processing_time(self, time) -> None:
        self._data_processing_time += time

    def increase_control_processing_time(self, time) -> None:
        self._control_processing_time += time

    def update_total_execution_time(self, time) -> None:
        self._total_execution_time = time
