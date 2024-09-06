package edu.uci.ics.amber.engine.common.storage.mongodb

import edu.uci.ics.amber.engine.common.storage.{BufferedItemWriter, VirtualDocument}

import java.net.URI
import scala.collection.mutable.ArrayBuffer

/**
  * MemoryDocument provides an in-memory implementation of VirtualDocument and BufferedItemWriter.
  * It stores items in a mutable ArrayBuffer and supports various operations like read, write, and remove.
  * @tparam T the type of data items stored in the document.
  */
class MemoryDocument[T >: Null <: AnyRef] extends VirtualDocument[T] with BufferedItemWriter[T] {

  /**
    * Internal storage for the document's items.
    */
  private val results = new ArrayBuffer[T]()

  /**
    * This method is not supported for MemoryDocument.
    * @throws UnsupportedOperationException always thrown when this method is called.
    * @return nothing, this method always throws an exception.
    */
  override def getURI: URI =
    throw new UnsupportedOperationException("getURI is not supported for MemoryDocument")

  /**
    * Remove all items from the document.
    */
  override def remove(): Unit =
    synchronized {
      results.clear()
    }

  /**
    * Get an iterator that iterates over all items in the document.
    * @return an iterator of items.
    */
  override def get(): Iterator[T] =
    synchronized {
      results.to(Iterator)
    }

  /**
    * Get the item at the specified index.
    * @param i the index of the item.
    * @return the item at the specified index.
    */
  override def getItem(i: Int): T =
    synchronized {
      results.apply(i)
    }

  /**
    * Get an iterator of items in the specified range.
    * @param from the starting index (inclusive).
    * @param until the ending index (exclusive).
    * @return an iterator of items in the specified range.
    */
  override def getRange(from: Int, until: Int): Iterator[T] =
    synchronized {
      results.slice(from, until).to(Iterator)
    }

  /**
    * Get an iterator of items starting after the specified offset.
    * @param offset the starting index (exclusive).
    * @return an iterator of items starting after the specified offset.
    */
  override def getAfter(offset: Int): Iterator[T] =
    synchronized {
      results.slice(offset, results.size).to(Iterator)
    }

  /**
    * Get the count of items in the document.
    * @return the number of items.
    */
  override def getCount: Long = results.length

  /**
    * Append an item to the document.
    * @param item the item to append.
    */
  override def append(item: T): Unit =
    synchronized {
      results += item
    }

  /**
    * Return a buffered item writer.
    * @return this MemoryDocument instance.
    */
  override def write(): BufferedItemWriter[T] = this

  /**
    * The size of the buffer for the buffered writer. This number is not used currently
    */
  override val bufferSize: Int = 1024

  /**
    * Open the writer. This method does nothing for MemoryDocument.
    */
  override def open(): Unit = {}

  /**
    * Close the writer. This method does nothing for MemoryDocument.
    */
  override def close(): Unit = {}

  /**
    * Put one item into the buffer. For MemoryDocument, it simply adds the item to the internal storage.
    * @param item the item to add.
    */
  override def putOne(item: T): Unit =
    synchronized {
      results += item
    }

  /**
    * Remove one item from the buffer. For MemoryDocument, it removes the item from the internal storage.
    * @param item the item to remove.
    */
  override def removeOne(item: T): Unit =
    synchronized {
      results -= item
    }
}
