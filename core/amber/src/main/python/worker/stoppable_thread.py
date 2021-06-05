from threading import Thread, Event

from loguru import logger


class StoppableThread(Thread):
    def __init__(self, name: str):
        super().__init__(name=name)
        self._running = Event()
        self._running.set()  # Set to running, initially

    def stop(self):
        self._running.clear()  # Set to False
        logger.debug(f"{self.name}-stopped")

    def running(self) -> bool:
        return self._running.isSet()
