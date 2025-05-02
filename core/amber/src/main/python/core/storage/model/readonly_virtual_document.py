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

from typing import Generic, TypeVar, Iterator

# Define a type variable
T = TypeVar("T")


class ReadonlyVirtualDocument(ABC, Generic[T]):
    """
    ReadonlyVirtualDocument provides an abstraction for read operations over a single
    resource.
    This class can be implemented by resources that only need to support read-related
    functionality.
    """

    @abstractmethod
    def get_uri(self) -> ParseResult:
        """
        Get the URI of the corresponding document.
        :return: the URI of the document as a ParseResult object
        """
        pass

    @abstractmethod
    def get_item(self, i: int) -> T:
        """
        Find the ith item and return.
        :param i: index starting from 0
        :return: data item of type T
        """
        pass

    @abstractmethod
    def get(self) -> Iterator[T]:
        """
        Get an iterator that iterates over all indexed items.
        :return: an iterator that returns data items of type T
        """
        pass

    @abstractmethod
    def get_range(self, from_index: int, until_index: int) -> Iterator[T]:
        """
        Get an iterator of a sequence starting from index `from_index`, until index
        `until`.
        :param from_index: the starting index (inclusive)
        :param until_index: the ending index (exclusive)
        :return: an iterator that returns data items of type T
        """
        pass

    @abstractmethod
    def get_after(self, offset: int) -> Iterator[T]:
        """
        Get an iterator of all items after the specified index `offset`.
        :param offset: the starting index (exclusive)
        :return: an iterator that returns data items of type T
        """
        pass

    @abstractmethod
    def get_count(self) -> int:
        """
        Get the count of items in the document.
        :return: the count of items
        """
        pass
