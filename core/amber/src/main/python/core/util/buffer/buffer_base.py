from abc import ABCMeta

from core.util.protocol.base_protocols import FlushedGetable, Putable


class IBuffer(FlushedGetable, Putable, metaclass=ABCMeta):
    pass
