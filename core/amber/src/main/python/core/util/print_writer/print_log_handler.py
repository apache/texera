from datetime import datetime
from typing import Iterator, List


class SimpleBuffer:
    def __init__(self, max_message_num=10, max_flush_interval_in_ms=500):
        self._max_message_num = max_message_num
        self._max_flush_interval_in_ms = max_flush_interval_in_ms
        self._buffer: List[str]() = list()
        self._last_output_time = datetime.now()

    def add(self, message: str) -> None:
        self._buffer.append(message)

    def output(self, flush=False) -> Iterator[str]:
        if (
            flush
            or len(self._buffer) >= self._max_message_num
            or (datetime.now() - self._last_output_time).seconds
            >= self._max_flush_interval_in_ms / 1000
        ):
            self._last_output_time = datetime.now()
            if self._buffer:
                yield "\n".join(self._buffer)
                self._buffer = list()
            else:
                return

    def __len__(self):
        return len(self._buffer)


class PrintLogHandler:
    def __init__(self, callback):
        self.callback = callback
        self._buffer = SimpleBuffer()

    def write(self, message: str):
        self._buffer.add(message)
        for msg in self._buffer.output():
            self.callback(msg)

    def flush(self):
        for msg in self._buffer.output(flush=True):
            self.callback(msg)
