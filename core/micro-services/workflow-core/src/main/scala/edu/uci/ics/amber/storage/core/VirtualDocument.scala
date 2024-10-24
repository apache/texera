package edu.uci.ics.amber.storage.core

import java.io.{File, InputStream}
import java.net.URI

/**
  * VirtualDocument provides the abstraction of doing read/write/copy/delete operations over a single resource in Texera system.
  * Note that all methods have a default implementation. This is because one document implementation may not be able to reasonably support all methods.
  * e.g. for dataset file, supports for read/write using file stream are essential, whereas read & write using index are hard to support and are semantically meaningless
 *
  * @tparam T the type of data that can use index to read and write.
  */
abstract class VirtualDocument[T] {

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
    * get an iterator that iterates all indexed items
    * @return an iterator that returns data items of type T
    */
  def get(): Iterator[T] = throw new NotImplementedError("get method is not implemented")

  /**
    * get an iterator of a sequence starting from index `from`, until index `until`
    * @param from the starting index (inclusive)
    * @param until the ending index (exclusive)
    * @return an iterator that returns data items of type T
    */
  def getRange(from: Int, until: Int): Iterator[T] =
    throw new NotImplementedError("getRange method is not implemented")

  /**
    * get an iterator of all items after the specified index `offset`
    * @param offset the starting index (exclusive)
    * @return an iterator that returns data items of type T
    */
  def getAfter(offset: Int): Iterator[T] =
    throw new NotImplementedError("getAfter method is not implemented")

  /**
    * get the count of items in the document
    * @return the count of items
    */
  def getCount: Long = throw new NotImplementedError("getCount method is not implemented")

  /**
    * set the ith item
    * @param i the index to set the item at
    * @param item the data item
    */
  def setItem(i: Int, item: T): Unit =
    throw new NotImplementedError("setItem method is not implemented")

  /**
    * return a writer that buffers the items and performs the flush operation at close time
    * @return a buffered item writer
    */
  def write(): BufferedItemWriter[T] =
    throw new NotImplementedError("write method is not implemented")

  /**
    * append one data item to the document
    * @param item the data item
    */
  def append(item: T): Unit =
    throw new NotImplementedError("append method is not implemented")

  /**
    * append data items from the iterator to the document
    * @param items iterator for the data items
    */
  def append(items: Iterator[T]): Unit =
    throw new NotImplementedError("append method is not implemented")

  /**
    * append the file content with an opened input stream
    * @param inputStream the data source input stream
    */
  def appendStream(inputStream: InputStream): Unit =
    throw new NotImplementedError("appendStream method is not implemented")

  /**
    * convert document to an input stream
    * @return the input stream
    */
  def asInputStream(): InputStream =
    throw new NotImplementedError("asInputStream method is not implemented")

  /**
    * convert document to a File
    * @return the file
    */
  def asFile(): File = throw new NotImplementedError("asFile method is not implemented")

  /**
    * physically remove the current document
    */
  def remove(): Unit
}
