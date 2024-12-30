package edu.uci.ics.amber.core.storage.result.iceberg.fileio

import org.apache.iceberg.io.{InputFile, SeekableInputStream}
import org.apache.iceberg.exceptions.RuntimeIOException

import java.io.{IOException, RandomAccessFile}
import java.nio.file.{Files, Paths}

/**
  * LocalInputFile is an Iceberg InputFile implementation for reading files from the local file system.*
  * @param path the local file path.
  */
class LocalInputFile(path: String) extends InputFile {

  /**
    * Returns the file's location as a string (its path).
    *
    * @return the file location.
    */
  override def location(): String = path

  /**
    * Gets the length of the file in bytes.
    *
    * @return the file length.
    * @throws RuntimeIOException if the file length cannot be determined.
    */
  override def getLength: Long = {
    try {
      Files.size(Paths.get(path))
    } catch {
      case e: IOException => throw new RuntimeIOException(e, s"Failed to get length of file: $path")
    }
  }

  /**
    * Opens a new seekable input stream for the file.
    *
    * @return a new SeekableFileInputStream instance.
    * @throws RuntimeIOException if the file cannot be opened.
    */
  override def newStream(): SeekableInputStream = {
    try {
      new SeekableFileInputStream(path)
    } catch {
      case e: IOException =>
        throw new RuntimeIOException(e, s"Failed to open file for reading: $path")
    }
  }

  /**
    * Checks whether the file exists on the local file system.
    *
    * @return true if the file exists, false otherwise.
    */
  override def exists(): Boolean = Files.exists(Paths.get(path))
}

/**
  * SeekableFileInputStream is a seekable input stream for reading files from the local file system.
  * - Implements Iceberg's SeekableInputStream for efficient random access.
  *
  * @param path the local file path.
  */
class SeekableFileInputStream(path: String) extends SeekableInputStream {

  // Underlying RandomAccessFile for low-level file operations
  private val file = new RandomAccessFile(path, "r")

  /**
    * Reads the next byte from the file.
    *
    * @return the next byte, or -1 if the end of the file is reached.
    */
  override def read(): Int = file.read()

  /**
    * Reads a sequence of bytes into the given array.
    *
    * @param b   the byte array to store data.
    * @param off the start offset in the array.
    * @param len the number of bytes to read.
    * @return the number of bytes read, or -1 if the end of the file is reached.
    */
  override def read(b: Array[Byte], off: Int, len: Int): Int = file.read(b, off, len)

  /**
    * Seeks to the specified position in the file.
    *
    * @param pos the position to seek to.
    */
  override def seek(pos: Long): Unit = file.seek(pos)

  /**
    * Gets the current position in the file.
    *
    * @return the current file pointer position.
    */
  override def getPos: Long = file.getFilePointer

  /**
    * Closes the input stream and releases any system resources.
    */
  override def close(): Unit = file.close()

  /**
    * Skips the specified number of bytes in the file.
    *
    * @param n the number of bytes to skip.
    * @return the actual number of bytes skipped.
    */
  override def skip(n: Long): Long = {
    val currentPos = file.getFilePointer
    val fileLength = file.length()
    val newPos = Math.min(currentPos + n, fileLength)
    file.seek(newPos)
    newPos - currentPos
  }
}
