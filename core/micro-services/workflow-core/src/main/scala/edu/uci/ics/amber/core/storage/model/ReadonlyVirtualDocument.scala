package edu.uci.ics.amber.core.storage.model

import java.io.{File, InputStream}
import java.net.URI

/**
  * ReadonlyVirtualDocument provides an abstraction for read operations over a single resource.
  * This trait can be implemented by resources that only need to support read-related functionality.
  * @tparam T the type of data that can use index to read.
  */
trait ReadonlyVirtualDocument[T] {

  /**
    * Get the URI of the corresponding document.
    * @return the URI of the document
    */
  def getURI: URI

  /**
    * Find ith item and return.
    * @param i index starting from 0
    * @return data item of type T
    */
  def getItem(i: Int): T

  /**
    * Get an iterator that iterates over all indexed items.
    * @return an iterator that returns data items of type T
    */
  def get(): Iterator[T]

  /**
    * Get an iterator of a sequence starting from index `from`, until index `until`.
    * @param from the starting index (inclusive)
    * @param until the ending index (exclusive)
    * @return an iterator that returns data items of type T
    */
  def getRange(from: Int, until: Int): Iterator[T]

  /**
    * Get an iterator of all items after the specified index `offset`.
    * @param offset the starting index (exclusive)
    * @return an iterator that returns data items of type T
    */
  def getAfter(offset: Int): Iterator[T]

  /**
    * Get the count of items in the document.
    * @return the count of items
    */
  def getCount: Long

  /**
    * Read document as an input stream.
    * @return the input stream
    */
  def asInputStream(): InputStream

  /**
    * Read or materialize document as an file
    */

  def asFile(): File
}
