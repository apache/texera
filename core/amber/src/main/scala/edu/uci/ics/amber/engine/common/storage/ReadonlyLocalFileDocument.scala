package edu.uci.ics.amber.engine.common.storage

import java.io.{File, FileInputStream, InputStream}
import java.net.URI

/**
  * ReadonlyLocalFileDocument provides a read-only abstraction over a local file.
  * Implements ReadonlyVirtualDocument without requiring a specific data type T.
  * Unsupported methods throw NotImplementedError.
  */
class ReadonlyLocalFileDocument(uri: URI) extends ReadonlyVirtualDocument[Nothing] {

  /**
    * Get the URI of the corresponding document.
    * @return the URI of the document
    */
  override def getURI: URI = uri

  /**
    * Get the file as an input stream for read operations.
    * @return InputStream to read from the file
    */
  override def asInputStream(): InputStream = new FileInputStream(new File(uri))

  /**
    * Get the file as an input stream for read operations.
    *
    * @return InputStream to read from the file
    */
  override def asFile(): File = new File(uri)

  /**
    * Find ith item and return.
    * For this implementation, items are unsupported, so this method is unimplemented.
    */
  override def getItem(i: Int): Nothing =
    throw new NotImplementedError("getItem is not supported for ReadonlyLocalFileDocument")

  /**
    * Get an iterator that iterates over all indexed items.
    * Unsupported in ReadonlyLocalFileDocument.
    */
  override def get(): Iterator[Nothing] =
    throw new NotImplementedError("get is not supported for ReadonlyLocalFileDocument")

  /**
    * Get an iterator of a sequence from index `from` to `until`.
    * Unsupported in ReadonlyLocalFileDocument.
    */
  override def getRange(from: Int, until: Int): Iterator[Nothing] =
    throw new NotImplementedError("getRange is not supported for ReadonlyLocalFileDocument")

  /**
    * Get an iterator of all items after the specified index `offset`.
    * Unsupported in ReadonlyLocalFileDocument.
    */
  override def getAfter(offset: Int): Iterator[Nothing] =
    throw new NotImplementedError("getAfter is not supported for ReadonlyLocalFileDocument")

  /**
    * Get the count of items in the document.
    * Unsupported in ReadonlyLocalFileDocument.
    */
  override def getCount: Long =
    throw new NotImplementedError("getCount is not supported for ReadonlyLocalFileDocument")
}