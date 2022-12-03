from abc import ABCMeta
from dataclasses import dataclass

from core.util.protocol.base_protocols import (
    Putable,
    Getable,
    EmtpyCheckable,
    KeyedPutable,
    KeyedEmtpyCheckable,
)


@dataclass
class QueueElement:
    pass


@dataclass
class QueueControl(QueueElement):
    msg: str


class IQueue(Putable, Getable, EmtpyCheckable, metaclass=ABCMeta):
    pass


class IKeyedQueue(KeyedPutable, Getable, KeyedEmtpyCheckable, metaclass=ABCMeta):
    pass
