package edu.uci.ics.amber.core.storage.result.iceberg.fileio

import org.apache.iceberg.io.{OutputFile, PositionOutputStream}
import org.apache.iceberg.exceptions.RuntimeIOException

import java.io.{File, FileOutputStream, IOException}
import java.nio.file.{Files, Paths, StandardOpenOption}

class LocalOutputFile(path: String) extends OutputFile {

  override def location(): String = path

  override def create(): PositionOutputStream = {
    try {
      new LocalPositionOutputStream(path, append = false)
    } catch {
      case e: IOException => throw new RuntimeIOException(e, s"Failed to create file: $path")
    }
  }

  override def createOrOverwrite(): PositionOutputStream = {
    try {
      new LocalPositionOutputStream(path, append = false)
    } catch {
      case e: IOException =>
        throw new RuntimeIOException(e, s"Failed to create or overwrite file: $path")
    }
  }

  override def toInputFile: LocalInputFile = new LocalInputFile(path)
}

class LocalPositionOutputStream(path: String, append: Boolean) extends PositionOutputStream {

  // Ensure the parent directories exist
  private val file = new File(path)
  if (!file.getParentFile.exists()) {
    file.getParentFile.mkdirs()
  }

  private val outputStream = new FileOutputStream(file, append)
  private var position: Long = 0

  override def write(b: Int): Unit = {
    outputStream.write(b)
    position += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    outputStream.write(b, off, len)
    position += len
  }

  override def getPos: Long = position

  override def close(): Unit = outputStream.close()
}
