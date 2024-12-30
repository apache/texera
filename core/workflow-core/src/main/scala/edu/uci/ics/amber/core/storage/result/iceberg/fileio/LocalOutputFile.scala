package edu.uci.ics.amber.core.storage.result.iceberg.fileio

import org.apache.iceberg.io.{OutputFile, PositionOutputStream}
import org.apache.iceberg.exceptions.RuntimeIOException

import java.io.{File, FileOutputStream, IOException}

/**
  * LocalOutputFile is an Iceberg OutputFile implementation for writing files to the local file system.
  *
  * @param path the local file path.
  */
class LocalOutputFile(path: String) extends OutputFile {

  /**
    * Returns the file's location as a string (its path).
    *
    * @return the file location.
    */
  override def location(): String = path

  /**
    * Creates a new file for writing.
    *
    * @return a new LocalPositionOutputStream instance.
    * @throws RuntimeIOException if the file cannot be created.
    */
  override def create(): PositionOutputStream = {
    try {
      new LocalPositionOutputStream(path, append = false)
    } catch {
      case e: IOException => throw new RuntimeIOException(e, s"Failed to create file: $path")
    }
  }

  /**
    * Creates or overwrites a file for writing.
    *
    * @return a new LocalPositionOutputStream instance.
    * @throws RuntimeIOException if the file cannot be created or overwritten.
    */
  override def createOrOverwrite(): PositionOutputStream = {
    try {
      new LocalPositionOutputStream(path, append = false)
    } catch {
      case e: IOException =>
        throw new RuntimeIOException(e, s"Failed to create or overwrite file: $path")
    }
  }

  /**
    * Converts this OutputFile to a LocalInputFile for reading.
    *
    * @return a LocalInputFile instance.
    */
  override def toInputFile: LocalInputFile = new LocalInputFile(path)
}

/**
  * LocalPositionOutputStream is a PositionOutputStream implementation for writing to local files.
  * - Tracks the write position to support Iceberg's file IO abstractions.
  * - Ensures parent directories are created before writing.
  *
  * @param path the local file path.
  * @param append whether to append to the file (if it exists).
  */
class LocalPositionOutputStream(path: String, append: Boolean) extends PositionOutputStream {

  // Ensure the parent directories exist
  private val file = new File(path)
  if (!file.getParentFile.exists()) {
    file.getParentFile.mkdirs()
  }

  private val outputStream = new FileOutputStream(file, append)
  private var position: Long = 0

  /**
    * Writes a single byte to the file and updates the position.
    *
    * @param b the byte to write.
    */
  override def write(b: Int): Unit = {
    outputStream.write(b)
    position += 1
  }

  /**
    * Writes a sequence of bytes to the file and updates the position.
    *
    * @param b   the byte array containing data.
    * @param off the start offset in the array.
    * @param len the number of bytes to write.
    */
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    outputStream.write(b, off, len)
    position += len
  }

  /**
    * Gets the current write position in the file.
    *
    * @return the current position in bytes.
    */
  override def getPos: Long = position

  /**
    * Closes the output stream and releases any system resources.
    */
  override def close(): Unit = outputStream.close()
}
