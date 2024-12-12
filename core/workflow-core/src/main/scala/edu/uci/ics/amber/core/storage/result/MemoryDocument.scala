package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Global storage mapping for all MemoryDocuments.
  * This ensures that 2 MemoryDocuments opened on the same key will operate on the same ArrayBuffer.
  */
object MemoryDocument {
  val storageMapping: mutable.Map[String, ArrayBuffer[AnyRef]] =
    mutable.HashMap[String, ArrayBuffer[AnyRef]]()
}

/**
  * MemoryDocument provides an in-memory implementation of VirtualDocument and BufferedItemWriter.
  * It stores items in a mutable ArrayBuffer and supports various operations like read, write, and remove.
  * @tparam T the type of data items stored in the document.
  */
class MemoryDocument[T >: Null <: AnyRef](key: String)
    extends VirtualDocument[T]
    with BufferedItemWriter[T] {

  /**
    * Internal storage for the document's items, retrieved from the shared mapping.
    */
  private def results: ArrayBuffer[T] = {
    if (!MemoryDocument.storageMapping.contains(key)) {
      MemoryDocument.storageMapping(key) = new ArrayBuffer[AnyRef]()
    }
    MemoryDocument.storageMapping(key).asInstanceOf[ArrayBuffer[T]]
  }

  override def getURI: URI =
    throw new UnsupportedOperationException("getURI is not supported for MemoryDocument")

  override def clear(): Unit =
    synchronized {
      results.clear()
    }

  override def get(): Iterator[T] =
    synchronized {
      results.to(Iterator)
    }

  override def getItem(i: Int): T =
    synchronized {
      results.apply(i)
    }

  override def getRange(from: Int, until: Int): Iterator[T] =
    synchronized {
      results.slice(from, until).to(Iterator)
    }

  override def getAfter(offset: Int): Iterator[T] =
    synchronized {
      results.slice(offset, results.size).to(Iterator)
    }

  override def getCount: Long = results.length

  override def append(item: T): Unit =
    synchronized {
      results += item
    }

  override def writer(): BufferedItemWriter[T] = this

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
    * @param item the data item to be written.
    */
  override def putOne(item: T): Unit =
    synchronized {
      results += item
    }

  /**
    * Remove one item from the buffer. For MemoryDocument, it removes the item from the internal storage.
    *
    * @param item the item to remove.
    */
  override def removeOne(item: T): Unit =
    synchronized {
      results -= item
    }
}
