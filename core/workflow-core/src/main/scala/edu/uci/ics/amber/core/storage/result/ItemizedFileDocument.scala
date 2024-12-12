package edu.uci.ics.amber.core.storage.result

import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import org.apache.commons.vfs2.{FileObject, VFS}

import java.io.{DataOutputStream, InputStream}
import java.net.URI
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer

object ItemizedFileDocument {
  // Initialize KryoPool as a static object
  private val kryoPool = KryoPool.withByteArrayOutputStream(10, new ScalaKryoInstantiator)
}

/**
  * ItemizedFileDocument provides methods to read/write items to a file located on the filesystem.
  * The type parameter T specifies the iterable data item stored in the file.
  *
  * @param uri the identifier of the file.
  *   If the file doesn't physically exist, ItemizedFileDocument will create the file(possibly also the parent folder)
  *      during its initialization.
  *   The lifecycle of the file is bundled with JVM, i.e. when JVM exits, the file gets deleted.
  */
class ItemizedFileDocument[T >: Null <: AnyRef](val uri: URI)
    extends VirtualDocument[T]
    with BufferedItemWriter[T] {

  val file: FileObject = VFS.getManager.resolveFile(uri)
  val lock = new ReentrantReadWriteLock()

  // Buffer to store items before flushing
  private val buffer = new ArrayBuffer[T]()
  override val bufferSize: Int = 1024

  // Register a shutdown hook to delete the file when the JVM exits
  sys.addShutdownHook {
    withWriteLock {
      if (file.exists()) {
        file.delete()
      }
    }
  }

  // Check and create the file if it does not exist
  withWriteLock {
    if (!file.exists()) {
      val parentDir = file.getParent
      if (parentDir != null && !parentDir.exists()) {
        parentDir.createFolder() // Create all necessary parent directories
      }
      file.createFile() // Create the file if it does not exist
    }
  }

  // Utility function to wrap code block with read lock
  private def withReadLock[M](block: => M): M = {
    lock.readLock().lock()
    try {
      block
    } finally {
      lock.readLock().unlock()
    }
  }

  // Utility function to wrap code block with write lock
  private def withWriteLock[M](block: => M): M = {
    lock.writeLock().lock()
    try {
      block
    } finally {
      lock.writeLock().unlock()
    }
  }

  /**
    * Utility function to get an iterator of data items of type T.
    * Each returned item will be deserialized using Kryo.
    */
  private def getIterator: Iterator[T] = {
    lazy val input = new com.twitter.chill.Input(file.getContent.getInputStream)
    new Iterator[T] {
      var record: T = internalNext()

      private def internalNext(): T = {
        try {
          val len = input.readInt()
          val bytes = input.readBytes(len)
          ItemizedFileDocument.kryoPool.fromBytes(bytes).asInstanceOf[T]
        } catch {
          case _: Throwable =>
            input.close()
            null
        }
      }

      override def next(): T = {
        val currentRecord = record
        record = internalNext()
        currentRecord
      }

      override def hasNext: Boolean = record != null
    }
  }

  /**
    * Append the content in the given object to the ItemizedFileDocument. This method is THREAD-SAFE.
    * Each record will be stored as <len of bytes><serialized bytes>.
    *
    * @param item the content to append
    */
  override def append(item: T): Unit =
    withWriteLock {
      buffer.append(item)
      if (buffer.size >= bufferSize) {
        flushBuffer()
      }
    }

  /**
    * Write buffered items to the file and clear the buffer.
    */
  private def flushBuffer(): Unit =
    withWriteLock {
      val outStream = file.getContent.getOutputStream(true)
      val dataOutStream = new DataOutputStream(outStream)
      try {
        buffer.foreach { item =>
          val serializedBytes = ItemizedFileDocument.kryoPool.toBytesWithClass(item)
          dataOutStream.writeInt(serializedBytes.length)
          dataOutStream.write(serializedBytes)
        }
        buffer.clear()
      } finally {
        dataOutStream.close()
        outStream.close()
      }
    }

  /**
    * Open the writer. Initializes the buffer.
    */
  override def open(): Unit =
    withWriteLock {
      buffer.clear()
    }

  /**
    * Close the writer, flushing any remaining buffered items to the file.
    */
  override def close(): Unit =
    withWriteLock {
      if (buffer.nonEmpty) {
        flushBuffer()
      }
    }

  /**
    * Put one item into the buffer. Flushes if the buffer is full.
    *
    * @param item the data item to be written
    */
  override def putOne(item: T): Unit = append(item)

  /**
    * Remove one item from the buffer. This does not affect items already written to the file.
    *
    * @param item the item to remove
    */
  override def removeOne(item: T): Unit =
    withWriteLock {
      buffer -= item
    }

  /**
    * Get the ith data item. The returned value will be deserialized using Kryo.
    *
    * @param i index starting from 0
    * @return data item of type T
    */
  override def getItem(i: Int): T =
    withReadLock {
      val iterator = getIterator
      iterator.drop(i).next()
    }

  override def getRange(from: Int, until: Int): Iterator[T] =
    withReadLock {
      getIterator.slice(from, until)
    }

  override def getAfter(offset: Int): Iterator[T] =
    withReadLock {
      getIterator.drop(offset + 1)
    }

  override def getCount: Long =
    withReadLock {
      getIterator.size
    }

  override def get(): Iterator[T] =
    withReadLock {
      getIterator
    }

  /**
    * Physically remove the file specified by the URI. This method is THREAD-SAFE.
    */
  override def clear(): Unit =
    withWriteLock {
      if (!file.exists()) {
        throw new RuntimeException(s"File $uri doesn't exist")
      }
      file.delete()
    }

  override def getURI: URI = uri
}
