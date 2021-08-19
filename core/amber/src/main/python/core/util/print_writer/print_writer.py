from datetime import datetime
from typing import Iterator, List


class SimpleBuffer:
    def __init__(self, maximum_size=10, maximum_window=2):
        self._max_size = maximum_size
        self._max_interval = maximum_window
        self._buffer: List[str]() = list()
        self._last_output_time = datetime.now()

    def add(self, message: str) -> None:
        self._buffer.append(message)

    def output(self, flush=False) -> Iterator[str]:
        if flush or len(self._buffer) >= self._max_size or (
                datetime.now() - self._last_output_time).seconds >= self._max_interval:
            self._last_output_time = datetime.now()
            if self._buffer:
                yield "\n".join(self._buffer)
                self._buffer = list()
            else:
                return

    def __len__(self):
        return len(self._buffer)


class PrintWriter:
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
