package edu.uci.ics.amber.engine.common.storage

import java.io.{File, InputStream}
import java.net.URI

/**
  * VirtualDocument provides the abstraction of doing read/write/copy/delete operations over a single resource in Texera system.
  * Note that all methods have a default implementation. This is because one document implementation may not be able to reasonably support all methods.
  * e.g. for dataset file, supports for read/write using file stream are essential, whereas read & write using index are hard to support and are semantically meaningless
  * @tparam T the type of data that can use index to read and write.
  */
abstract class VirtualDocument[T >: Null <: AnyRef] {

  def getURI: URI

  /**
    * read ith item and return
    * @param i index starting from 0
    * @return data item of type T
    */
  def readItem(i: Int): T =
    throw new UnsupportedOperationException("readItem method is not supported")

  /**
    * iterate over whole document using iterator
    * @return an iterator that return data item of type T
    */
  def read(): Iterator[T] = throw new UnsupportedOperationException("read method is not supported")

  /**
    * append one data item to the document
    * @param item the data item
    */
  def writeItem(item: T): Unit =
    throw new UnsupportedOperationException("writeItem method is not supported")

  /**
    * append data items from the iterator to the document
    * @param items iterator for the data item
    */
  def write(items: Iterator[T]): Unit =
    throw new UnsupportedOperationException("write method is not supported")

  /**
    * overwrite the file content with an opened input stream
    * @param inputStream the data source input stream
    */
  def writeWithStream(inputStream: InputStream): Unit =
    throw new UnsupportedOperationException("writeWithStream method is not supported")

  /**
    * read the document as an input stream
    *
    * @return the input stream
    */
  def asInputStream(): InputStream =
    throw new UnsupportedOperationException("asInputStream method is not supported")

  def asFile(): File = throw new UnsupportedOperationException("asFile method is not supported")

  /**
    * physically remove current document
    */
  def remove(): Unit
}
