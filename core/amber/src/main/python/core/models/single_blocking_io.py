# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from threading import Condition
from types import TracebackType
from typing import IO, Type, AnyStr, Iterator, Iterable, Optional


class SingleBlockingIO(IO):
    """
    An implementation of single-element IO that can be blocked on reading.

    Some highlights:
    - The IO only has one value.
    - Each write() will append to the value.
    - Each flush() will make the value readable resets the value to be written next.
    - Each readline() will fetch the value and clear the IO.
    - When there is no value to read, it blocks in readline() until there is a value.
    """

    def __init__(self, condition: Condition):
        self.value: Optional[str] = None
        self.buf: str = ""
        self.condition: Condition = condition

    def write(self, s: str) -> None:
        """
        Writes a partial string, append to the buffer.
        :param s: a string.
        :return:
        """
        self.buf += s

    def flush(self) -> None:
        """
        Denotes the end of buffer, adds a "\n" to complete the string.
        Flushes the completed string in the buffer to value.
        Resets the buffer to accept the next complete string.
        :return:
        """
        self.write("\n")
        self.value, self.buf = self.buf, ""

    def readline(self, limit=None) -> str:
        """
        Fetches a string value by removing it from the IO. It blocks the current
        thread until there is a valid string to fetch.
        :param limit: parent's API, not implemented here. It is always None.
        :return str: A completed string value.
        """
        try:
            with self.condition:
                # keeps waiting until a value is available
                while self.value is None:
                    self.condition.notify()
                    self.condition.wait()

                # noinspection PyTypeChecker
                return self.value
        finally:
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
