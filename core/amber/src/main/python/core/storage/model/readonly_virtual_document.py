from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Iterator
from urllib.parse import ParseResult  # Python's URI equivalent
import io  # For InputStream equivalent
import os  # For File equivalent

# Define a type variable
T = TypeVar("T")


class ReadonlyVirtualDocument(ABC, Generic[T]):
    """
    ReadonlyVirtualDocument provides an abstraction for read operations over a single
    esource.
    This class can be implemented by resources that only need to support read-related
    functionality.

    :param T: the type of data that can be accessed via an index.
    """

    @abstractmethod
    def get_uri(self) -> ParseResult:
        """
        Get the URI of the corresponding document.
        :return: the URI of the document
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
    def get_range(self, from_index: int, until: int) -> Iterator[T]:
        """
        Get an iterator of a sequence starting from index `from_index`, until index
        `until`.
        :param from_index: the starting index (inclusive)
        :param until: the ending index (exclusive)
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

    @abstractmethod
    def as_input_stream(self) -> io.IOBase:
        """
        Read the document as an input stream.
        :return: the input stream
        """
        pass

    @abstractmethod
    def as_file(self) -> os.PathLike:
        """
        Read or materialize the document as a file.
        :return: the file object
        """
        pass
