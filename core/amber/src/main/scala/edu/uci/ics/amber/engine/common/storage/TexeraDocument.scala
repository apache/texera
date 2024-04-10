package edu.uci.ics.amber.engine.common.storage

import java.io.{InputStream, OutputStream}

/**
  * TexeraDocument provides the abstraction of doing read/write/copy/delete operations over a resource in Texera system.
  * Note that all methods have a default implementation. This is because one document implementation may not be able to reasonably support all methods.
  * e.g. for dataset file, supports for read/write using file stream are essential, whereas read & write using index are hard to support and are semantically meaningless
  * @tparam T the type of data that can use index to read and write.
  */
abstract class TexeraDocument[T >: Null <: AnyRef] {

  def getURI: TexeraURI

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
    * make the document red by an opened outputStream
    * @param outputStream stream to put the document in
    */
  def readAsOutputStream(outputStream: OutputStream): Unit =
    throw new UnsupportedOperationException("readAsOutputStream method is not supported")

  /**
    * read the document as an input stream
    * @return the input stream
    */
  def readAsInputStream(): InputStream =
    throw new UnsupportedOperationException("readAsInputStream method is not supported")

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
    * copy the document to another location specified by URI.
    * @param to
    * - if to is None, the implementation will decide a location to stored the duplicated document
    * - if to is given, the duplicated document will be there
    * @return the uri identifying the duplicated document
    */
  def copy(to: Option[TexeraURI] = None): TexeraURI =
    throw new UnsupportedOperationException("copy is not supported")

  /**
    * physically remove current document
    */
  def rm(): Unit
}
