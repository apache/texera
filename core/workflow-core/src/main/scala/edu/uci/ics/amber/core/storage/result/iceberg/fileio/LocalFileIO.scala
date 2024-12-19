package edu.uci.ics.amber.core.storage.result.iceberg.fileio

import org.apache.iceberg.io.{FileIO, InputFile, OutputFile}
import org.apache.iceberg.exceptions.RuntimeIOException

import java.io.{File, IOException, RandomAccessFile}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.atomic.AtomicBoolean

class LocalFileIO extends FileIO {

  private val isClosed = new AtomicBoolean(false)

  override def newInputFile(path: String): InputFile = {
    ensureNotClosed()
    new LocalInputFile(path)
  }

  override def newOutputFile(path: String): OutputFile = {
    ensureNotClosed()
    new LocalOutputFile(path)
  }

  override def deleteFile(path: String): Unit = {
    ensureNotClosed()
    try {
      Files.deleteIfExists(Paths.get(path))
    } catch {
      case e: IOException => throw new RuntimeIOException(e, s"Failed to delete file: $path")
    }
  }

  override def initialize(properties: java.util.Map[String, String]): Unit = {
    // No special initialization required for local file system
  }

  override def close(): Unit = {
    isClosed.set(true)
  }

  private def ensureNotClosed(): Unit = {
    if (isClosed.get()) {
      throw new IllegalStateException("Cannot use LocalFileIO after it has been closed")
    }
  }
}
