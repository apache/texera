from datetime import datetime

from typing import Tuple, List, Iterator

from proto.edu.uci.ics.amber.engine.architecture.worker import PythonConsoleMessageV2


class TimedBuffer:
    def __init__(self, max_message_num=10, max_flush_interval_in_ms=500):
        self._max_message_num = max_message_num
        self._max_flush_interval_in_ms = max_flush_interval_in_ms
        self._buffer: List[Tuple[datetime, str]]() = list()
        self._last_output_time = datetime.now()

    def add(self, console_message: PythonConsoleMessageV2) -> None:
        self._buffer.append(console_message)

    def get(self, flush=False) -> Iterator[PythonConsoleMessageV2]:
        if (
            flush
            or len(self._buffer) >= self._max_message_num
            or (datetime.now() - self._last_output_time).seconds
            >= self._max_flush_interval_in_ms / 1000
        ):
            self._last_output_time = datetime.now()
            yield from self._buffer
            self._buffer.clear()
