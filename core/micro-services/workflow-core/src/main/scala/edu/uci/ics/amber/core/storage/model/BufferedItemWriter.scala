package edu.uci.ics.amber.core.storage.model

/**
  * BufferedItemWriter provides an interface for writing items to a buffer and performing I/O operations.
  * The items are buffered before being written to the underlying storage to optimize performance.
  * @tparam T the type of data items to be written.
  */
trait BufferedItemWriter[T] {

  /**
    * The size of the buffer.
    * @return the buffer size.
    */
  val bufferSize: Int

  /**
    * Open the writer, initializing any necessary resources.
    * This method should be called before any write operations.
    */
  def open(): Unit

  /**
    * Close the writer, flushing any remaining items in the buffer
    * to the underlying storage and releasing any held resources.
    */
  def close(): Unit

  /**
    * Put one item into the buffer. If the buffer is full, it should be flushed to the underlying storage.
    * @param item the data item to be written.
    */
  def putOne(item: T): Unit

  /**
    * Remove one item from the buffer. If the item is not found in the buffer, an appropriate action should be taken,
    * such as throwing an exception or ignoring the request.
    * @param item the data item to be removed.
    */
  def removeOne(item: T): Unit
}
