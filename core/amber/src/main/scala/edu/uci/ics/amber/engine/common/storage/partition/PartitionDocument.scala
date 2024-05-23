package edu.uci.ics.amber.engine.common.storage.partition

import edu.uci.ics.amber.engine.common.storage.{FileDocument, VirtualDocument}

import java.net.URI

/**
  * PartitionDocument is a storage object that consists #numOfPartition physical files as its underlying data storage.
  * Each underlying file's URI is in the format of {partitionDocumentURI}_{index}.
  *
  * PartitionDocument only support getting the FileDocument that corresponds to the single partition either by index or by iterator.
  * To write over the partition, you should get the FileDocument first, then call write-related methods over it. FileDocument guarantees the thread-safe read/write.
  *
  * The Type parameter T is used to specify the type of data item stored in the partition
  * @param uri the id of this partition document. Note that this URI does not physically corresponds to a file.
  * @param numOfPartition number of partitions
  */
class PartitionDocument[T >: Null <: AnyRef](val uri: URI, val numOfPartition: Int)
    extends VirtualDocument[FileDocument[T]] {

  /**
    * Utility functions to generate the partition URI by index
    * @param i index of the partition
    * @return the URI of the partition
    */
  private def getPartitionURI(i: Int): URI = {
    if (i < 0 || i >= numOfPartition) {
      throw new RuntimeException(f"Index $i out of bound")
    }
    new URI(s"${uri}_$i")
  }

  override def getURI: URI =
    throw new RuntimeException(
      "Partition Document doesn't physically exist. It is invalid to acquire its URI"
    )

  /**
    * Get the partition by index i.
    * This method is THREAD-UNSAFE, as multiple threads can get any partition by index. But the returned FileDocument is thread-safe
    * @param i index starting from 0
    * @return FileDocument corresponds to the certain partition
    */
  override def getItem(i: Int): FileDocument[T] = {
    new FileDocument(getPartitionURI(i))
  }

  /**
    * Get the iterator of partitions.
    * This method is THREAD-UNSAFE, as multiple threads can get the iterator and loop through all partitions. But the returned FileDocument is thread-safe
    *  @return an iterator that return the FileDocument corresponds to the certain partition
    */
  override def get(): Iterator[FileDocument[T]] =
    new Iterator[FileDocument[T]] {
      private var i: Int = 0

      override def hasNext: Boolean = i < numOfPartition

      override def next(): FileDocument[T] = {
        if (!hasNext) {
          throw new NoSuchElementException("No more partitions")
        }
        val document = new FileDocument[T](getPartitionURI(i))
        i += 1
        document
      }
    }

  /**
    * Remove all partitions.
    * This method is THREAD-UNSAFE. But FileDocument's remove is thread-safe
    */
  override def remove(): Unit = {
    for (i <- 0 until numOfPartition) {
      getItem(i).remove()
    }
  }
}
