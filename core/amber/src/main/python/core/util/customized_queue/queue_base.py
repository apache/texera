from abc import abstractmethod
from dataclasses import dataclass
from typing import TypeVar

from typing_extensions import Protocol

T = TypeVar("T")


class IQueue(Protocol):
    @dataclass
    class QueueElement:
        pass

    @dataclass
    class QueueControl(QueueElement):
        msg: str

    @abstractmethod
    def is_empty(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    def get(self, *args, **kwargs) -> T:
        pass

    @abstractmethod
    def put(self, *args, **kwargs) -> None:
        pass
