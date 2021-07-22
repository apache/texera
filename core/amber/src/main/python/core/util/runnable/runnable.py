from abc import abstractmethod
from typing import Protocol


class Runnable(Protocol):

    @abstractmethod
    def run(self):
        """run some logic"""
