package edu.uci.ics.amber.core.storage.result.iceberg

import org.apache.iceberg.Files.{localInput, localOutput}
import org.apache.iceberg.exceptions.RuntimeIOException
import org.apache.iceberg.io.{FileIO, InputFile, OutputFile}

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicBoolean

/**
  * LocalFileIO is a custom FileIO implementation for Iceberg to use the local file system
  * for storing data and metadata files.
  */
class LocalFileIO extends FileIO {

  // Tracks whether the FileIO instance is closed
  private val isClosed = new AtomicBoolean(false)

  /**
    * Creates an InputFile for reading from the local file system.
    *
    * @param path the file path.
    * @return a new LocalInputFile instance.
    * @throws IllegalStateException if the FileIO is closed.
    */
  override def newInputFile(path: String): InputFile = {
    ensureNotClosed()
    localInput(path)
  }

  /**
    * Creates an OutputFile for writing to the local file system.
    *
    * @param path the file path.
    * @return a new LocalOutputFile instance.
    * @throws IllegalStateException if the FileIO is closed.
    */
  override def newOutputFile(path: String): OutputFile = {
    ensureNotClosed()
    localOutput(path)
  }

  /**
    * Deletes a file from the local file system.
    *
    * @param path the file path to delete.
    * @throws RuntimeIOException if the deletion fails.
    * @throws IllegalStateException if the FileIO is closed.
    */
  override def deleteFile(path: String): Unit = {
    ensureNotClosed()
    try {
      Files.deleteIfExists(Paths.get(path))
    } catch {
      case e: IOException => throw new RuntimeIOException(e, s"Failed to delete file: $path")
    }
  }

  /**
    * Initializes the FileIO with properties.
    * - No special initialization is required for local file systems.
    *
    * @param properties configuration properties (unused).
    */
  override def initialize(properties: java.util.Map[String, String]): Unit = {
    // No special initialization required for local file system
  }

  /**
    * Marks the FileIO as closed, preventing further operations.
    */
  override def close(): Unit = {
    isClosed.set(true)
  }

  /**
    * Ensures the FileIO is not closed before performing operations.
    *
    * @throws IllegalStateException if the FileIO is closed.
    */
  private def ensureNotClosed(): Unit = {
    if (isClosed.get()) {
      throw new IllegalStateException("Cannot use LocalFileIO after it has been closed")
    }
  }
}
