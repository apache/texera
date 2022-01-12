from abc import abstractmethod
from dataclasses import dataclass

from typing_extensions import Protocol, T


class IQueue(Protocol):
    @dataclass
    class QueueElement:
        pass

    @dataclass
    class QueueControl(QueueElement):
        msg: str

    @abstractmethod
    def empty(self) -> bool:
        pass

    @abstractmethod
    def get(self) -> T:
        pass

    @abstractmethod
    def put(self, item: T) -> None:
        pass
