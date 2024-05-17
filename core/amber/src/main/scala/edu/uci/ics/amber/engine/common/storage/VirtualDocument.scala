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

  /**
    * get the URI of corresponding document
    * @return the URI of the document
    */
  def getURI: URI

  /**
    * find ith item and return
    * @param i index starting from 0
    * @return data item of type T
    */
  def getItem(i: Int): T =
    throw new NotImplementedError("getItem method is not implemented")

  /**
    * get a iterator that iterates all indexed items
    * @return an iterator that return data item of type T
    */
  def get(): Iterator[T] = throw new NotImplementedError("get method is not implemented")

  /**
    * append one data item to the document
    * @param item the data item
    */
  def setItem(item: T): Unit =
    throw new NotImplementedError("setItem method is not implemented")

  /**
    * set ith item
    * @param item the data item
    */
  def setItem(i: Int, item: T): Unit =
    throw new NotImplementedError("setItem method is not implemented")

  /**
    * append data items from the iterator to the document
    * @param items iterator for the data item
    */
  def write(items: Iterator[T]): Unit =
    throw new NotImplementedError("write method is not implemented")

  /**
    * append the file content with an opened input stream
    * @param inputStream the data source input stream
    */
  def write(inputStream: InputStream): Unit =
    throw new NotImplementedError("write method is not implemented")

  /**
    * convert document as an input stream
    * @return the input stream
    */
  def asInputStream(): InputStream =
    throw new NotImplementedError("asInputStream method is not implemented")

  /**
    * convert document as a File
    * @return the file
    */
  def asFile(): File = throw new NotImplementedError("asFile method is not implemented")

  /**
    * physically remove current document
    */
  def remove(): Unit
}
