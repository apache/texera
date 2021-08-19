from datetime import datetime
from typing import Iterator, List


class SimpleBuffer:
    def __init__(self, maximum_size=10, maximum_window=2):
        self.maximum_size = maximum_size
        self.maximum_window = maximum_window
        self.buffer: List[str]() = list()
        self.last_output_time = datetime.now()

    def add(self, message: str) -> None:
        self.buffer.append(message)

    def output(self, flush=False) -> Iterator[str]:
        if flush or len(self.buffer) >= self.maximum_size or (
                datetime.now() - self.last_output_time).seconds >= self.maximum_window:
            self.last_output_time = datetime.now()
            yield "\n".join(self.buffer)
            self.buffer = list()


class PrintWriter:
    def __init__(self, callback):
        self.callback = callback
        self._buffer = SimpleBuffer()

    def write(self, message: str):
        self._buffer.add(message)
        for msg in self._buffer.output():
            self.callback(msg)
