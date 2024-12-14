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
) extends VirtualDocument[T]
    with BufferedItemWriter[T] {

  private val file: FileObject = VFS.getManager.resolveFile(uri)
  private val lock = new ReentrantReadWriteLock()
  private val buffer = new ArrayBuffer[T]()
  override val bufferSize: Int = 1024

  private var arrowRootallocator: RootAllocator = _
  private var arrowVectorSchemaRoot: VectorSchemaRoot = _
  private var arrowFileWriter: ArrowFileWriter = _

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

  private def withReadLock[M](block: => M): M = {
    lock.readLock().lock()
    try block
    finally lock.readLock().unlock()
  }

  private def withWriteLock[M](block: => M): M = {
    lock.writeLock().lock()
    try block
    finally lock.writeLock().unlock()
  }

  override def open(): Unit =
    withWriteLock {
      buffer.clear()
      arrowRootallocator = new RootAllocator()
      arrowVectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, arrowRootallocator)
      val outputStream = new FileOutputStream(file.getURL.getPath)
      arrowFileWriter = new ArrowFileWriter(arrowVectorSchemaRoot, null, outputStream.getChannel)
      arrowFileWriter.start()
    }

  override def putOne(item: T): Unit =
    withWriteLock {
      buffer.append(item)
      if (buffer.size >= bufferSize) {
        flushBuffer()
      }
    }

  override def removeOne(item: T): Unit =
    withWriteLock {
      buffer -= item
    }

  private def flushBuffer(): Unit =
    withWriteLock {
      if (buffer.nonEmpty) {
        buffer.zipWithIndex.foreach {
          case (item, index) =>
            serializer(item, index, arrowVectorSchemaRoot)
        }
        arrowVectorSchemaRoot.setRowCount(buffer.size)
        arrowFileWriter.writeBatch()
        buffer.clear()
        arrowVectorSchemaRoot.clear()
      }
    }

  override def close(): Unit =
    withWriteLock {
      if (buffer.nonEmpty) {
        flushBuffer()
      }
      if (arrowFileWriter != null) {
        arrowFileWriter.end()
        arrowFileWriter.close()
      }
      if (arrowVectorSchemaRoot != null) arrowVectorSchemaRoot.close()
      if (arrowRootallocator != null) arrowRootallocator.close()
    }

  override def get(): Iterator[T] =
    withReadLock {
      val path = Paths.get(file.getURL.toURI)
      val allocator = new RootAllocator()
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

  override def getURI: URI = uri

  override def clear(): Unit =
    withWriteLock {
      if (file.exists()) {
        file.delete()
      } else {
        throw new RuntimeException(s"File $uri doesn't exist")
      }
    }
}
