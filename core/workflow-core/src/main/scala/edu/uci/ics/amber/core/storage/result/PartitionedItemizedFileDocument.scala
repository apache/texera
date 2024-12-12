  package edu.uci.ics.amber.core.storage.result

  import edu.uci.ics.amber.core.storage.model.VirtualDocument
  import edu.uci.ics.amber.core.storage.result.PartitionedItemizedFileDocument.getPartitionURI
  import edu.uci.ics.amber.util.PathUtils.workflowResultsRootPath
  import org.apache.commons.vfs2.VFS

  import java.net.URI
  import java.util.concurrent.locks.ReentrantLock


  object PartitionedItemizedFileDocument {

    /**
     * Utility function to generate the partition URI by index.
     */
    def getPartitionURI(id: String, i: Int): URI = {
      workflowResultsRootPath.resolve(id).resolve(s"partition$i").toUri
    }
  }

  class PartitionedItemizedFileDocument[T >: Null <: AnyRef](val id: String, val numOfPartition: Int)
      extends VirtualDocument[T] {

    // The array of itemized file documents, each stands for a partition
    private val partitions =
      Array.tabulate(numOfPartition)(i => new ItemizedFileDocument[T](getPartitionURI(id, i)))

    // Cursor for each partition to track read position
    private val cursors = Array.fill(numOfPartition)(0)

    private val mutex = new ReentrantLock()

    // use round-robin to decide which partition to go to when reading i-th item
    private def getPartitionIndex(i: Int): Int = i % numOfPartition

    override def getURI: URI =
      workflowResultsRootPath.resolve(id).toUri

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

    override def getRange(from: Int, until: Int): Iterator[T] = {
      mutex.lock()
      try {
        get().slice(from, until)
      } finally {
        mutex.unlock()
      }
    }

    override def getAfter(offset: Int): Iterator[T] = {
      mutex.lock()
      try {
        get().drop(offset + 1)
      } finally {
        mutex.unlock()
      }
    }

    override def getCount: Long = {
      mutex.lock()
      try {
        partitions.map(_.getCount).sum
      } finally {
        mutex.unlock()
      }
    }

    override def clear(): Unit = {
      mutex.lock()
      try {
        for (partition <- partitions) {
          partition.clear()
        }
        VFS.getManager.resolveFile(getURI).delete()
      } finally {
        mutex.unlock()
      }
    }
  }
