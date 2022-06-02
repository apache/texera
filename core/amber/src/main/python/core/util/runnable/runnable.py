from abc import abstractmethod

from typing_extensions import Protocol


class Runnable(Protocol):
    @abstractmethod
    def run(self) -> None:
        """run some logic"""
