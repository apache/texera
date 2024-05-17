package edu.uci.ics.amber.engine.common.storage
import org.apache.commons.vfs2.{FileObject, VFS}

import java.io.{InputStream, OutputStreamWriter}
import java.net.URI
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
  * FileDocument provides methods to read/write a file located on filesystem.
  * All methods are THREAD-SAFE implemented using read-write lock:
  * - 1 writer at a time: only 1 thread of current JVM can acquire the write lock
  * - n reader at a time: multiple threads of current JVM can acquire the read lock
  * @param uri the identifier of the file. If file doesn't physically exist, FileDocument will create the file during the constructing phase.
  */
class FileDocument(val uri: URI) extends VirtualDocument[String] {
  val file: FileObject = VFS.getManager.resolveFile(uri.toString)
  val lock = new ReentrantReadWriteLock()

  // Utility function to wrap code block with read lock
  private def withReadLock[T](block: => T): T = {
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
    * @param inputStream the data source input stream
    */
  override def write(inputStream: InputStream): Unit =
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
    * Append the content in the given string to the FileDocument. This method is THREAD-SAFE
    * @param item the content to append
    */
  override def setItem(item: String): Unit =
    withWriteLock {
      val outStream = file.getContent.getOutputStream(true)
      val writer = new OutputStreamWriter(outStream)
      try {
        writer.write(item)
        writer.flush()
      } finally {
        writer.close()
        outStream.close()
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
