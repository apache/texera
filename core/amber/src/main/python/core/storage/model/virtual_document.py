import io
import os
from abc import ABC, abstractmethod
from urllib.parse import ParseResult

from core.storage.model.buffered_item_writer import BufferedItemWriter
from core.storage.model.readonly_virtual_document import ReadonlyVirtualDocument
from overrides import overrides
from typing import TypeVar, Iterator

# Define a type variable
T = TypeVar("T")


class VirtualDocument(ReadonlyVirtualDocument[T], ABC):
    """
    VirtualDocument provides the abstraction of performing read/write/copy/delete
    operations over a single resource.
    Note that all methods have a default implementation. This is because one document
    implementation may not be able
    to reasonably support all methods.

    :param T: the type of data that can use index to read and write.
    """

    @abstractmethod
    def get_uri(self) -> ParseResult:
        """
        Get the URI of the corresponding document.
        :return: the URI of the document
        """
        pass

    @overrides
    def get_item(self, i: int) -> T:
        raise NotImplementedError("get_item method is not implemented")

    @overrides
    def get(self) -> Iterator[T]:
        raise NotImplementedError("get method is not implemented")

    @overrides
    def get_range(self, from_index: int, until: int) -> Iterator[T]:
        raise NotImplementedError("get_range method is not implemented")

    @overrides
    def get_after(self, offset: int) -> Iterator[T]:
        raise NotImplementedError("get_after method is not implemented")

    @overrides
    def get_count(self) -> int:
        raise NotImplementedError("get_count method is not implemented")

    def set_item(self, i: int, item: T) -> None:
        raise NotImplementedError("set_item method is not implemented")

    def writer(self, writer_identifier: str) -> "BufferedItemWriter[T]":
        raise NotImplementedError("writer method is not implemented")

    def append(self, item: T) -> None:
        raise NotImplementedError("append method is not implemented")

    def append_items(self, items: Iterator[T]) -> None:
        raise NotImplementedError("append_items method is not implemented")

    def append_stream(self, input_stream: io.IOBase) -> None:
        raise NotImplementedError("append_stream method is not implemented")

    def as_input_stream(self) -> io.IOBase:
        raise NotImplementedError("as_input_stream method is not implemented")

    def as_file(self) -> os.PathLike:
        raise NotImplementedError("as_file method is not implemented")

    @abstractmethod
    def clear(self) -> None:
        """
        Physically remove the current document.
        """
        pass
