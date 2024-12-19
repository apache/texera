package edu.uci.ics.amber.core.storage.result.iceberg.fileio

import org.apache.iceberg.io.{InputFile, SeekableInputStream}
import org.apache.iceberg.exceptions.RuntimeIOException

import java.io.{File, FileInputStream, IOException, RandomAccessFile}
import java.nio.file.{Files, Paths}

class LocalInputFile(path: String) extends InputFile {

  override def location(): String = path

  override def getLength: Long = {
    try {
      Files.size(Paths.get(path))
    } catch {
      case e: IOException => throw new RuntimeIOException(e, s"Failed to get length of file: $path")
    }
  }

  override def newStream(): SeekableInputStream = {
    try {
      new SeekableFileInputStream(path)
    } catch {
      case e: IOException =>
        throw new RuntimeIOException(e, s"Failed to open file for reading: $path")
    }
  }

  override def exists(): Boolean = Files.exists(Paths.get(path))
}

class SeekableFileInputStream(path: String) extends SeekableInputStream {

  private val file = new RandomAccessFile(path, "r")

  override def read(): Int = file.read()

  override def read(b: Array[Byte], off: Int, len: Int): Int = file.read(b, off, len)

  override def seek(pos: Long): Unit = file.seek(pos)

  override def getPos: Long = file.getFilePointer

  override def close(): Unit = file.close()

  override def skip(n: Long): Long = {
    val currentPos = file.getFilePointer
    val fileLength = file.length()
    val newPos = Math.min(currentPos + n, fileLength)
    file.seek(newPos)
    newPos - currentPos
  }
}
