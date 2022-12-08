from abc import abstractmethod
from typing import TypeVar, Sized, Optional
from typing_extensions import Protocol

T = TypeVar("T")
K = TypeVar("K")


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


class FlushedGetable(Protocol):
    @abstractmethod
    def get(self, flush: bool) -> T:
        pass


class EmtpyCheckable(Sized):
    @abstractmethod
    def is_empty(self) -> bool:
        pass


class KeyedEmtpyCheckable(Sized):
    @abstractmethod
    def is_empty(self, key: Optional[K] = None) -> bool:
        pass
