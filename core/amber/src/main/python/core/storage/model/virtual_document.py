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
from urllib.parse import ParseResult

from overrides import overrides
from typing import TypeVar, Iterator

from core.storage.model.buffered_item_writer import BufferedItemWriter
from core.storage.model.readonly_virtual_document import ReadonlyVirtualDocument

# Define a type variable
T = TypeVar("T")


class VirtualDocument(ReadonlyVirtualDocument[T], ABC):
    """
    VirtualDocument provides the abstraction of performing read/write/copy/delete
    operations over a single resource.
    Note that all methods have a default implementation. This is because one document
    implementation may not be able to reasonably support all methods.
    """

    @overrides
    def get_uri(self) -> ParseResult:
        raise NotImplementedError("get_uri method is not implemented")

    @overrides
    def get_item(self, i: int) -> T:
        raise NotImplementedError("get_item method is not implemented")

    @overrides
    def get(self) -> Iterator[T]:
        raise NotImplementedError("get method is not implemented")

    @overrides
    def get_range(self, from_index: int, until_index: int) -> Iterator[T]:
        raise NotImplementedError("get_range method is not implemented")

    @overrides
    def get_after(self, offset: int) -> Iterator[T]:
        raise NotImplementedError("get_after method is not implemented")

    @overrides
    def get_count(self) -> int:
        raise NotImplementedError("get_count method is not implemented")

    def writer(self, writer_identifier: str) -> BufferedItemWriter[T]:
        """
        return a writer that buffers the items and performs the flush operation at
        close time
        :param writer_identifier: the id of the writer
        :return: a buffered item writer
        """
        raise NotImplementedError("writer method is not implemented")

    @abstractmethod
    def clear(self) -> None:
        """
        Physically remove the current document.
        """
        pass
