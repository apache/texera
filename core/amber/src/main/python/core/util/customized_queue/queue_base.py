from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from typing import TypeVar, Sized, Optional

from typing_extensions import Protocol

T = TypeVar("T")
K = TypeVar("K")


@dataclass
class QueueElement:
    pass


@dataclass
class QueueControl(QueueElement):
    msg: str


class Putable(Protocol):
    @abstractmethod
    def put(self, item: T) -> None:
        pass


class KeyedPutable(Protocol):
    @abstractmethod
    def put(self, key: K, item: T) -> None:
        pass


class Getable(Protocol):
    @abstractmethod
    def get(self) -> T:
        pass


class EmtpyCheckable(Sized):
    @abstractmethod
    def is_empty(self) -> bool:
        pass


class KeyedEmtpyCheckable(Sized):
    @abstractmethod
    def is_empty(self, key: Optional[K] = None) -> bool:
        pass


class IQueue(Putable, Getable, EmtpyCheckable, metaclass=ABCMeta):
    pass


class IKeyedQueue(KeyedPutable, Getable, KeyedEmtpyCheckable, metaclass=ABCMeta):
    pass
