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

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

# Define a type variable
T = TypeVar("T")


class BufferedItemWriter(ABC, Generic[T]):
    """
    BufferedItemWriter provides an interface for writing items to a buffer and
    performing I/O operations.
    The items are buffered before being written to the underlying storage to
    optimize performance.
    """

    @property
    @abstractmethod
    def buffer_size(self) -> int:
        """
        The size of the buffer.
        :return: the buffer size.
        """
        pass

    @abstractmethod
    def open(self) -> None:
        """
        Open the writer, initializing any necessary resources.
        This method should be called before any write operations.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close the writer, flushing any remaining items in the buffer
        to the underlying storage and releasing any held resources.
        """
        pass

    @abstractmethod
    def put_one(self, item: T) -> None:
        """
        Put one item into the buffer. If the buffer is full, it should be flushed to
        the underlying storage.
        :param item: the data item to be written.
        """
        pass

    @abstractmethod
    def remove_one(self, item: T) -> None:
        """
        Remove one item from the buffer. If the item is not found in the buffer, an
        appropriate action should be taken,
        such as throwing an exception or ignoring the request.
        :param item: the data item to be removed.
        """
        pass
