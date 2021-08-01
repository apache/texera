import typing


class StatisticsManager:
    def __init__(self):
        self._input_tuple_count = 0
        self._output_tuple_count = 0

    def get_statistics(self) -> typing.Tuple[int, int]:
        return self._input_tuple_count, self._output_tuple_count

    def increase_input_tuple_count(self) -> None:
        self._input_tuple_count += 1

    def increase_output_tuple_count(self) -> None:
        self._output_tuple_count += 1
