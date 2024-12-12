package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter}
import org.apache.arrow.vector.{VectorSchemaRoot, FieldVector}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.commons.vfs2.{FileObject, VFS}

import java.io.{FileInputStream, FileOutputStream, DataOutputStream}
import java.net.URI
import java.nio.channels.{FileChannel, SeekableByteChannel}
import java.nio.file.{Paths, StandardOpenOption}
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer
import scala.util.Using

class ArrowFileDocument[T](
                            val uri: URI,
                            val arrowSchema: Schema,
                            val serializer: (T, Int, VectorSchemaRoot) => Unit,
                            val deserializer: (Int, VectorSchemaRoot) => T
                          ) extends VirtualDocument[T] with BufferedItemWriter[T] {

  private val file: FileObject = VFS.getManager.resolveFile(uri)
  private val lock = new ReentrantReadWriteLock()
  private val allocator = new RootAllocator()
  private val buffer = new ArrayBuffer[T]()
  override val bufferSize: Int = 1024

  // Initialize the file if it doesn't exist
  withWriteLock {
    if (!file.exists()) {
      val parentDir = file.getParent
      if (parentDir != null && !parentDir.exists()) {
        parentDir.createFolder()
      }
      file.createFile()
    }
  }

  // Utility function to wrap code block with read lock
  private def withReadLock[M](block: => M): M = {
    lock.readLock().lock()
    try block
    finally lock.readLock().unlock()
  }

  // Utility function to wrap code block with write lock
  private def withWriteLock[M](block: => M): M = {
    lock.writeLock().lock()
    try block
    finally lock.writeLock().unlock()
  }

  override def putOne(item: T): Unit = withWriteLock {
    buffer.append(item)
    if (buffer.size >= bufferSize) {
      flushBuffer()
    }
  }

  override def removeOne(item: T): Unit = withWriteLock {
    buffer -= item
  }

  /** Write buffered items to the file and clear the buffer */
  private def flushBuffer(): Unit = withWriteLock {
    val outputStream = new FileOutputStream(file.getURL.getPath, true)
    Using.Manager { use =>
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val writer = new ArrowFileWriter(root, null, outputStream.getChannel)
      use(writer)
      use(root)

      writer.start()

      buffer.zipWithIndex.foreach { case (item, index) =>
        serializer(item, index, root)
      }

      root.setRowCount(buffer.size)
      writer.writeBatch()
      buffer.clear()
      writer.end()
    }
  }

  /** Open the writer (clear the buffer) */
  override def open(): Unit = withWriteLock {
    buffer.clear()
  }

  /** Close the writer, flushing any remaining buffered items */
  override def close(): Unit = withWriteLock {
    if (buffer.nonEmpty) {
      flushBuffer()
    }
    allocator.close()
  }

  /** Get an iterator of data items of type T */
  private def getIterator: Iterator[T] = withReadLock {
    val path = Paths.get(file.getURL.toURI)
    val channel: SeekableByteChannel = FileChannel.open(path, StandardOpenOption.READ)
    val reader = new ArrowFileReader(channel, allocator)
    val root = reader.getVectorSchemaRoot

    new Iterator[T] {
      private var currentIndex = 0
      private var currentBatchLoaded = reader.loadNextBatch()

      private def loadNextBatch(): Boolean = {
        currentBatchLoaded = reader.loadNextBatch()
        currentIndex = 0
        currentBatchLoaded
      }

      override def hasNext: Boolean = currentIndex < root.getRowCount || loadNextBatch()

      override def next(): T = {
        if (!hasNext) throw new NoSuchElementException("No more elements")
        val item = deserializer(currentIndex, root)
        currentIndex += 1
        item
      }
    }
  }

  /** Get the ith data item */
  override def getItem(i: Int): T = withReadLock {
    getIterator.drop(i).next()
  }

  /** Get a range of data items */
  override def getRange(from: Int, until: Int): Iterator[T] = withReadLock {
    getIterator.slice(from, until)
  }

  /** Get items after a certain offset */
  override def getAfter(offset: Int): Iterator[T] = withReadLock {
    getIterator.drop(offset + 1)
  }

  /** Get the total count of items */
  override def getCount: Long = withReadLock {
    getIterator.size
  }

  /** Get all items as an iterator */
  override def get(): Iterator[T] = withReadLock {
    getIterator
  }

  /** Physically remove the file */
  override def clear(): Unit = withWriteLock {
    if (file.exists()) {
      file.delete()
    } else {
      throw new RuntimeException(s"File $uri doesn't exist")
    }
  }

  override def getURI: URI = uri
}