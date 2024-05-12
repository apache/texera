package edu.uci.ics.amber.engine.common.storage.partition

import edu.uci.ics.amber.engine.common.storage.{FileDocument, VirtualDocument}

import java.net.URI

// PartitionDocument is a storage object that consists #numOfPartition physical files as its underlying data storage
// Each underlying file's URI is in the format of {partitionDocumentURI}_{index}
class PartitionDocument(val uri: URI, val numOfPartition: Int)
    extends VirtualDocument[FileDocument] {
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

  override def readItem(i: Int): FileDocument = {
    new FileDocument(getPartitionURI(i))
  }

  override def read(): Iterator[FileDocument] =
    new Iterator[FileDocument] {
      private var i: Int = 0

      override def hasNext: Boolean = i < numOfPartition

      override def next(): FileDocument = {
        if (!hasNext) {
          throw new NoSuchElementException("No more partitions")
        }
        val document = new FileDocument(getPartitionURI(i))
        i += 1
        document
      }
    }

  override def remove(): Unit = {
    for (i <- 0 until numOfPartition) {
      readItem(i).remove()
    }
  }
}
