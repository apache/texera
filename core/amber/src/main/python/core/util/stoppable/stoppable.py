from abc import abstractmethod
from typing_extensions import Protocol


class Stoppable(Protocol):
    @abstractmethod
    def stop(self):
        """stop self"""
