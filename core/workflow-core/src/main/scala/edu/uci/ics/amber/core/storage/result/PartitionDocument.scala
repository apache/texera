package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.storage.model.VirtualDocument

import java.net.URI
import java.util.concurrent.locks.ReentrantLock

class PartitionDocument[T >: Null <: AnyRef](val id: String, val numOfPartition: Int)
    extends VirtualDocument[T] {

  // Array of partitions
  private val partitions =
    Array.tabulate(numOfPartition)(i => new ItemizedFileDocument[T](getPartitionURI(i)))

  // Cursor for each partition to track read position
  private val cursors = Array.fill(numOfPartition)(0)

  // Mutex for thread safety
  private val mutex = new ReentrantLock()

  /**
    * Utility function to generate the partition URI by index.
    * @param i Index of the partition.
    * @return The URI of the partition.
    */
  private def getPartitionURI(i: Int): URI = {
    if (i < 0 || i >= numOfPartition) {
      throw new RuntimeException(s"Index $i out of bounds")
    }
    new URI(s"${id}_partition$i")
  }

  // use round-robin to decide which partition to go to
  private def getPartitionIndex(i: Int): Int = i % numOfPartition

  override def getURI: URI =
    throw new RuntimeException(
      "Partition Document doesn't physically exist. It is invalid to acquire its URI"
    )

  /**
    * Get the partition item by index.
    * @param i Index starting from 0.
    * @return The data item of type T.
    */
  override def getItem(i: Int): T = {
    mutex.lock()
    try {
      val partitionIndex = getPartitionIndex(i)
      val document = partitions(partitionIndex)
      val item = document.getItem(cursors(partitionIndex))
      cursors(partitionIndex) += 1
      item
    } finally {
      mutex.unlock()
    }
  }

  override def get(): Iterator[T] =
    new Iterator[T] {
      private var partitionIndex = 0
      private val iterators = partitions.map(_.get())

      override def hasNext: Boolean = iterators.exists(_.hasNext)

      override def next(): T = {
        mutex.lock()
        try {
          while (!iterators(partitionIndex).hasNext) {
            partitionIndex = getPartitionIndex(partitionIndex + 1)
          }
          iterators(partitionIndex).next()
        } finally {
          mutex.unlock()
        }
      }
    }

  /**
    * Get an iterator over a range of items.
    * @param from The starting index (inclusive).
    * @param until The ending index (exclusive).
    * @return An iterator over the specified range of items.
    */
  override def getRange(from: Int, until: Int): Iterator[T] = {
    mutex.lock()
    try {
      get().slice(from, until)
    } finally {
      mutex.unlock()
    }
  }

  /**
    * Get an iterator over all items after the specified index.
    * @param offset The starting index (exclusive).
    * @return An iterator over the items after the specified offset.
    */
  override def getAfter(offset: Int): Iterator[T] = {
    mutex.lock()
    try {
      get().drop(offset + 1)
    } finally {
      mutex.unlock()
    }
  }

  /**
    * Get the total count of items across all partitions.
    * @return The total count of items.
    */
  override def getCount: Long = {
    mutex.lock()
    try {
      partitions.map(_.getCount).sum
    } finally {
      mutex.unlock()
    }
  }

  /**
    * Remove all partitions.
    * This method is thread-safe due to the mutex.
    */
  override def clear(): Unit = {
    mutex.lock()
    try {
      for (partition <- partitions) {
        partition.clear()
      }
    } finally {
      mutex.unlock()
    }
  }
}
