package edu.uci.ics.amber.engine.common.storage
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import edu.uci.ics.amber.engine.common.storage.FileDocument.kryoPool
import org.apache.commons.vfs2.{FileObject, VFS}

import java.io.InputStream
import java.net.URI
import java.util.concurrent.locks.ReentrantReadWriteLock

object FileDocument {
  // Initialize KryoPool as a static object
  private val kryoPool = KryoPool.withByteArrayOutputStream(10, new ScalaKryoInstantiator)
}

/**
  * FileDocument provides methods to read/write a file located on filesystem.
  * All methods are THREAD-SAFE implemented using read-write lock:
  * - 1 writer at a time: only 1 thread of current JVM can acquire the write lock
  * - n reader at a time: multiple threads of current JVM can acquire the read lock
  *
  * The type parameter T is used to specify the iterable data item stored in the File. FileDocument provides easy ways of setting/iterating these data items
  *
  * @param uri the identifier of the file. If file doesn't physically exist, FileDocument will create the file during the constructing phase.
  */
class FileDocument[T >: Null <: AnyRef](val uri: URI) extends VirtualDocument[T] {
  val file: FileObject = VFS.getManager.resolveFile(uri.toString)
  val lock = new ReentrantReadWriteLock()

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
  private def withWriteLock(block: => Unit): Unit = {
    lock.writeLock().lock()
    try {
      block
    } finally {
      lock.writeLock().unlock()
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

  /**
    * Append the content in the inputStream to the FileDocument. This method is THREAD-SAFE
    * This method will NOT do any serialization. So the it is invalid to use getItem and iterator to get T from the document.
    * @param inputStream the data source input stream
    */
  override def appendStream(inputStream: InputStream): Unit =
    withWriteLock {
      val outStream = file.getContent.getOutputStream(true)
      try {
        // create a buffer for reading from inputStream
        val buffer = new Array[Byte](1024)
        // create an Iterator to repeatedly call inputStream.read, and direct buffered data to file
        Iterator
          .continually(inputStream.read(buffer))
          .takeWhile(_ != -1)
          .foreach(outStream.write(buffer, 0, _))
      } finally {
        outStream.close()
      }
    }

  /**
    * Append the content in the given object to the FileDocument. This method is THREAD-SAFE
    * Each record will be stored as <len of bytes><serialized bytes>.
    * @param item the content to append
    */
  override def append(item: T): Unit =
    withWriteLock {
      val outStream = file.getContent.getOutputStream(true)
      val dataOutStream = new java.io.DataOutputStream(outStream)
      try {
        // write the length and the raw bytes in
        val serializedBytes = kryoPool.toBytesWithClass(item)
        dataOutStream.writeInt(serializedBytes.length)
        dataOutStream.write(serializedBytes)
      } finally {
        dataOutStream.close()
        outStream.close()
      }
    }

  /**
    * get the ith data item. The returned value will be deserialized using kyro
    *
    * @param i index starting from 0
    * @return data item of type T
    */
  override def getItem(i: Int): T = {
    val iterator = get()
    iterator.drop(i).next()
  }

  /**
    * get the iterator of data items of type T. Each returned item will be deserialized using kyro
    *  @return an iterator that return data item of type T
    */
  override def get(): Iterator[T] = {
    lazy val input = new com.twitter.chill.Input(asInputStream())
    new Iterator[T] {
      var record: T = internalNext()

      private def internalNext(): T = {
        try {
          val len = input.readInt()
          val bytes = input.readBytes(len)
          kryoPool.fromBytes(bytes).asInstanceOf[T]
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
    * Read content in the file document as the InputStream. This method is THREAD-SAFE
    *  @return the input stream of content in the FileDocument. Due to the constraint of getInputStream, there may be only 1 input/output stream at any time
    */
  override def asInputStream(): InputStream =
    withReadLock {
      if (!file.exists()) {
        throw new RuntimeException(f"File $uri doesn't exist")
      }
      file.getContent.getInputStream
    }

  override def getURI: URI = uri

  /**
    * Physically remove the file specified by the URI. This method is THREAD-SAFE
    */
  override def remove(): Unit =
    withWriteLock {
      if (!file.exists()) {
        throw new RuntimeException(f"File $uri doesn't exist")
      }
      file.delete()
    }
}
