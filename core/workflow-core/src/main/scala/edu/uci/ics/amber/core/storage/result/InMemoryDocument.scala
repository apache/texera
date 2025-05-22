package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.amber.core.storage.util.StorageUtil.withWriteLock

import java.net.URI
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer

/**
 * InMemoryDocument is a lightweight, thread-safe implementation of VirtualDocument
 * that stores records entirely in memory. It supports concurrent readers and
 * a single writer with write locks.
 *
 * @param deserde Function to deserialize an internal representation to T.
 * @tparam T type of data stored in memory.
 */
private[storage] class InMemoryDocument[T >: Null <: AnyRef](
                                                            ) extends VirtualDocument[T] {

  private val data = ArrayBuffer[Any]()
  private val lock = new ReentrantReadWriteLock()

  override def getURI: URI = URI.create("inmemory://document")

  override def clear(): Unit =
    withWriteLock(lock) {
      data.clear()
    }

  override def get(): Iterator[T] = getRange(0, data.size)

  override def getRange(from: Int, until: Int): Iterator[T] = {
    lock.readLock().lock()
    try {
      data.slice(from, until).iterator.map(_.asInstanceOf[T])
    } finally {
      lock.readLock().unlock()
    }
  }

  override def getAfter(offset: Int): Iterator[T] = getRange(offset, data.size)

  override def getCount: Long = {
    lock.readLock().lock()
    try data.size
    finally lock.readLock().unlock()
  }

  override def writer(writerIdentifier: String): BufferedItemWriter[T] = new BufferedItemWriter[T] {

    override val bufferSize: Int = 1000 // you can customize this or make it configurable

    private val writeBuffer = new ArrayBuffer[T](bufferSize)
    private var isOpen = false

    override def open(): Unit = withWriteLock(lock) {
      isOpen = true
      writeBuffer.clear()
    }

    override def putOne(item: T): Unit = withWriteLock(lock) {
      if (!isOpen) throw new IllegalStateException("Writer not opened.")
      writeBuffer.append(item)
      if (writeBuffer.size >= bufferSize) {
        flushBuffer()
      }
    }

    override def removeOne(item: T): Unit = withWriteLock(lock) {
      if (!isOpen) throw new IllegalStateException("Writer not opened.")
      writeBuffer -= item
    }

    override def close(): Unit = withWriteLock(lock) {
      flushBuffer()
      isOpen = false
    }

    private def flushBuffer(): Unit = {
      withWriteLock(lock) {
        data.appendAll(writeBuffer)
      }
      writeBuffer.clear()
    }
  }

}
