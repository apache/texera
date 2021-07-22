from abc import abstractmethod
from typing import Protocol


class Stoppable(Protocol):

    @abstractmethod
    def stop(self):
        """stop self"""
