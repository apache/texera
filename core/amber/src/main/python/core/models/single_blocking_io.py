from __future__ import annotations

from threading import Condition, current_thread
from types import TracebackType
from typing import IO, Type, AnyStr, Iterator, Iterable

from loguru import logger


class SingleBlockingIO(IO):
    def __init__(self, condition: Condition):
        self.value = None
        self.condition = condition

    def write(self, v):
        if self.value is None:
            self.value = ""

        self.value += v
        logger.info("Now value is:" + repr(self.value))

    def flush(self):
        if self.value is None:
            self.value = ""
        self.value += "\n"
        logger.info("calling flush " + str(current_thread()))
        with self.condition:
            logger.info(
                "done with flush " + str(current_thread()) + ": " + repr(self.value)
            )
            self.condition.notify()
            logger.info("waiting after flush " + str(current_thread()))
            self.condition.wait()

    def readline(self, limit=None):
        logger.info("calling readline " + str(current_thread()))
        try:
            with self.condition:
                if self.value is None:
                    logger.info("waiting on read " + str(current_thread()))
                    self.condition.wait()
                logger.info(
                    "done with read " + str(current_thread()) + ": " + repr(self.value)
                )
                return self.value + "\n"
        finally:
            logger.info("set value to None")
            self.value = None

    ####################################################################################
    # The following IO methods are not implemented as they are not used in pdb.
    ####################################################################################
    def close(self) -> None:
        pass

    def fileno(self) -> int:
        pass

    def isatty(self) -> bool:
        pass

    def read(self, __n: int = ...) -> AnyStr:
        pass

    def readable(self) -> bool:
        pass

    def readlines(self, __hint: int = ...) -> list[AnyStr]:
        pass

    def seek(self, __offset: int, __whence: int = ...) -> int:
        pass

    def seekable(self) -> bool:
        pass

    def tell(self) -> int:
        pass

    def truncate(self, __size: int | None = ...) -> int:
        pass

    def writable(self) -> bool:
        pass

    def writelines(self, __lines: Iterable[AnyStr]) -> None:
        pass

    def __next__(self) -> AnyStr:
        pass

    def __iter__(self) -> Iterator[AnyStr]:
        pass

    def __enter__(self) -> IO[AnyStr]:
        pass

    def __exit__(
        self,
        __t: Type[BaseException] | None,
        __value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        pass
